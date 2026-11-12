#include "rpcchannel.h"
#include "rpccontroller.h"
#include "zookeeperutil.h"

#include <mymuduo/Logger.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>

#include <arpa/inet.h>

static const int64_t kMprpcChannelDefaultTimeoutMs = 30000;

// MprpcChannel 通过 RpcChannel 提供异步请求/响应映射
MprpcChannel::MprpcChannel(EventLoop *loop, const InetAddress &serverAddr)
    : loop_(loop),
      client_(loop, serverAddr, "MprpcChannel"),
      callbackOwner_(new CallbackOwner()),
      m_reqId(1)
{
    // 回调可能在连接生命周期内晚到，先锁弱引用，避免捕获裸 this
    const std::shared_ptr<CallbackOwner> callbackOwner = callbackOwner_;
    client_.setConnectionCallback(
        [callbackOwner](const TcpConnectionPtr &conn)
        {
            std::shared_ptr<MprpcChannel> channel;
            {
                std::lock_guard<std::mutex> lock(callbackOwner->mutex);
                channel = callbackOwner->channel.lock();
            }
            if (channel)
            {
                channel->onConnection(conn);
            }
        });
    client_.setMessageCallback(
        [callbackOwner](const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time)
        {
            std::shared_ptr<MprpcChannel> channel;
            {
                std::lock_guard<std::mutex> lock(callbackOwner->mutex);
                channel = callbackOwner->channel.lock();
            }
            if (channel)
            {
                channel->onMessage(conn, buffer, time);
            }
        });
    client_.enableRetry();
    client_.connect();
}

MprpcChannel::~MprpcChannel()
{
    std::vector<std::pair<uint64_t, std::shared_ptr<PendingCall>>> pendingCalls;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        pendingCalls.reserve(m_pendingCalls.size());
        for (auto &entry : m_pendingCalls)
        {
            pendingCalls.emplace_back(entry.first, std::move(entry.second));
        }
        m_pendingCalls.clear();
        m_unsentPackets.clear();
    }

    for (const auto &entry : pendingCalls)
    {
        const uint64_t reqId = entry.first;
        const std::shared_ptr<PendingCall> &call = entry.second;
        loop_->cancel(call->timerId);
        sendCancelInLoop(reqId, call);

        if (call->controller)
        {
            call->controller->SetFailed("rpc channel destroyed");
            if (MprpcController *rpcController =
                    dynamic_cast<MprpcController *>(call->controller))
            {
                rpcController->setCancelCallback(std::function<void()>());
            }
        }
        if (call->done)
        {
            call->done->Run();
        }
    }
}

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                              google::protobuf::RpcController *controller,
                              const google::protobuf::Message *request,
                              google::protobuf::Message *response,
                              google::protobuf::Closure *done)
{
    if (!method || !request || !response)
    {
        if (controller)
        {
            controller->SetFailed("invalid RPC method or message");
        }
        if (done)
        {
            done->Run();
        }
        return;
    }

    if (controller && controller->IsCanceled())
    {
        controller->SetFailed("rpc canceled");
        if (done)
        {
            done->Run();
        }
        return;
    }

    const uint64_t req_id = m_reqId++;

    RpcFrameHeader header;
    header.set_service_name(method->service()->name());
    header.set_method_name(method->name());
    header.set_req_id(req_id);

    std::string args;
    if (!request->SerializeToString(&args))
    {
        if (controller)
        {
            controller->SetFailed("serialize request failed");
        }
        if (done)
        {
            done->Run();
        }
        return;
    }
    std::string packet;
    int64_t timeoutMs = kMprpcChannelDefaultTimeoutMs;
    if (auto *rpcController = dynamic_cast<MprpcController *>(controller))
    {
        if (rpcController->RpcTimeoutMs() > 0)
        {
            timeoutMs = rpcController->RpcTimeoutMs();
        }
        if (rpcController->HasDeadline())
        {
            const int64_t remainingMs = rpcController->RemainingMs();
            if (remainingMs <= 0)
            {
                rpcController->SetFailed("rpc deadline exceeded");
                if (done)
                {
                    done->Run();
                }
                return;
            }
            timeoutMs = std::min(timeoutMs, remainingMs);
        }
        header.set_trace_id(rpcController->traceId());
    }
    timeoutMs = std::min<int64_t>(timeoutMs,
                                  std::numeric_limits<uint32_t>::max());

    header.set_args_size(static_cast<uint32_t>(args.size()));
    header.set_raw_size(static_cast<uint32_t>(args.size()));
    header.set_deadline_ms(static_cast<uint32_t>(timeoutMs));
    const size_t bodyOffset = BeginEncodeRpcFrame(header, args.size(), &packet);
    if (bodyOffset == kRpcFrameInvalidOffset)
    {
        if (controller)
        {
            controller->SetFailed("request exceeds frame limits");
        }
        if (done)
        {
            done->Run();
        }
        return;
    }
    if (!args.empty())
    {
        std::memcpy(&packet[bodyOffset], args.data(), args.size());
    }

    std::weak_ptr<MprpcChannel> weak_this;
    try
    {
        weak_this = shared_from_this();
    }
    catch (const std::bad_weak_ptr &)
    {
        if (controller)
        {
            controller->SetFailed("MprpcChannel must be owned by std::shared_ptr");
        }
        if (done)
        {
            done->Run();
        }
        return;
    }
    {
        // 仅在 CallMethod 可取得 shared_ptr 后发布 owner，TCP 回调据此安全锁定对象
        std::lock_guard<std::mutex> lock(callbackOwner_->mutex);
        callbackOwner_->channel = weak_this;
    }

    auto call = std::make_shared<PendingCall>();
    call->controller = controller;
    call->response = response;
    call->done = done;

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pendingCalls[req_id] = call;
    }

    if (auto *rpcController = dynamic_cast<MprpcController *>(controller))
    {
        rpcController->setCancelCallback(
            [weak_this, req_id]()
            {
                if (auto self = weak_this.lock())
                {
                    self->onCancelCallback(req_id);
                }
            });
    }

    // 先发布 pending 项，避免极短定时器在注册请求前触发并丢失超时事件
    TimerId timer = loop_->runAfter(
        static_cast<double>(timeoutMs) / 1000.0,
        [weak_this, req_id]()
        {
            if (auto self = weak_this.lock())
            {
                self->onTimeout(req_id);
            }
        });
    bool stillPending = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto pendingCallEntry = m_pendingCalls.find(req_id);
        if (pendingCallEntry != m_pendingCalls.end() &&
            pendingCallEntry->second == call)
        {
            call->timerId = timer;
            stillPending = true;
        }
    }
    if (!stillPending)
    {
        loop_->cancel(timer);
        return;
    }

    loop_->queueInLoop([weak_this, req_id, packet]()
                       {
                           if (auto self = weak_this.lock())
                           {
                               self->sendRequestInLoop(req_id, packet);
                           } });
}

void MprpcChannel::onConnection(const TcpConnectionPtr &conn)
{
    // 断线时所有在途请求都无法再收到响应，统一取消定时器并通知调用方
    if (conn->connected())
    {
        responseFrame_.reset();
        LOG_INFO("MprpcChannel connected to %s", conn->peerAddress().toIpPort().c_str());

        std::vector<std::pair<uint64_t, std::string>> unsent;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            unsent.swap(m_unsentPackets);
        }

        for (const auto &queuedPacket : unsent)
        {
            sendRequestInLoop(queuedPacket.first, queuedPacket.second);
        }
    }
    else
    {
        responseFrame_.reset();
        LOG_INFO("MprpcChannel disconnected from %s",
                 conn->peerAddress().toIpPort().c_str());

        std::vector<std::shared_ptr<PendingCall>> pending;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            for (auto pendingCallEntry = m_pendingCalls.begin();
                 pendingCallEntry != m_pendingCalls.end();)
            {
                if (pendingCallEntry->second->sent)
                {
                    pending.push_back(pendingCallEntry->second);
                    pendingCallEntry = m_pendingCalls.erase(pendingCallEntry);
                }
                else
                {
                    ++pendingCallEntry;
                }
            }
        }
        for (const auto &call : pending)
        {
            loop_->cancel(call->timerId);
            if (call->controller)
            {
                call->controller->SetFailed("connection closed");
            }
            if (call->done)
            {
                call->done->Run();
            }
        }
        if (disconnectedCallback_)
        {
            disconnectedCallback_();
        }
    }
}
void MprpcChannel::onMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time)
{
    // 响应按帧增量解析，完整帧可能一次收到多条
    (void)time;
    for (;;)
    {
        const RpcFrameFeedResult result = FeedRpcFrame(
            buffer->peek(), buffer->readableBytes(), &responseFrame_);
        if (result == RpcFrameFeedResult::kNeedMore)
        {
            return;
        }
        if (result == RpcFrameFeedResult::kError)
        {
            LOG_ERROR("malformed RPC response from %s",
                      conn->peerAddress().toIpPort().c_str());
            buffer->retrieveAll();
            responseFrame_.reset();
            conn->shutdown();
            return;
        }

        const RpcFrameHeader header = responseFrame_.header;
        const size_t bodyOffset = 4 + responseFrame_.headerSize;
        const size_t bodySize = header.args_size();
        std::string body(buffer->peek() + bodyOffset, bodySize);
        const size_t totalSize = responseFrame_.totalSize();
        buffer->retrieve(totalSize);
        responseFrame_.reset();

        if ((header.flags() & kRpcFrameFlagCompressed) != 0)
        {
            std::string decompressed(header.raw_size(), '\0');
            size_t decompressedSize = 0;
            if (!DecompressRpcFrameBody(body.data(), body.size(),
                                        &decompressed[0], decompressed.size(),
                                        &decompressedSize) ||
                decompressedSize != header.raw_size())
            {
                LOG_ERROR("RPC response decompression failed (req_id=%lu)",
                          static_cast<unsigned long>(header.req_id()));
                RpcFrameHeader failedHeader = header;
                failedHeader.set_err_code(kRpcFrameBadRequest);
                failedHeader.set_err_msg("response decompression failed");
                onResponse(failedHeader, std::string());
                continue;
            }
            body = std::move(decompressed);
        }
        onResponse(header, body);
    }
}
void MprpcChannel::onResponse(const RpcFrameHeader &header,
                              const std::string &body)
{
    // req_id 将响应关联到原始调用，查找不到时说明调用已超时或取消
    uint64_t req_id = header.req_id();

    std::shared_ptr<PendingCall> call = takePendingCall(req_id);
    if (!call)
    {
        LOG_INFO("no pending call for req_id=%lu", (unsigned long)req_id);
        return;
    }

    loop_->cancel(call->timerId);

    if (header.err_code() != 0)
    {
        if (call->controller)
            call->controller->SetFailed(header.err_msg());
    }
    else
    {
        if (!call->response->ParseFromString(body))
        {
            if (call->controller)
                call->controller->SetFailed("parse response error");
        }
    }

    if (call->done)
    {
        call->done->Run();
    }
}

std::shared_ptr<MprpcChannel::PendingCall>
MprpcChannel::takePendingCall(uint64_t req_id)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto pendingCallEntry = m_pendingCalls.find(req_id);
    if (pendingCallEntry == m_pendingCalls.end())
    {
        return std::shared_ptr<PendingCall>();
    }

    std::shared_ptr<PendingCall> call = pendingCallEntry->second;
    m_pendingCalls.erase(pendingCallEntry);
    m_unsentPackets.erase(
        std::remove_if(m_unsentPackets.begin(), m_unsentPackets.end(),
                       [req_id](const std::pair<uint64_t, std::string> &packet)
                       { return packet.first == req_id; }),
        m_unsentPackets.end());
    return call;
}

void MprpcChannel::sendRequestInLoop(uint64_t req_id, const std::string &packet)
{
    TcpConnectionPtr conn = client_.connection();
    bool shouldSend = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto pendingCallEntry = m_pendingCalls.find(req_id);
        if (pendingCallEntry == m_pendingCalls.end() || pendingCallEntry->second->sent)
        {
            return;
        }
        if (conn && conn->connected())
        {
            pendingCallEntry->second->queued = false;
            pendingCallEntry->second->sent = true;
            pendingCallEntry->second->connection = conn;
            shouldSend = true;
        }
        else if (!pendingCallEntry->second->queued)
        {
            pendingCallEntry->second->queued = true;
            m_unsentPackets.push_back(std::make_pair(req_id, packet));
        }
    }

    if (shouldSend)
    {
        conn->send(packet);
    }
}

void MprpcChannel::onCancelCallback(uint64_t req_id)
{
    // RpcController 可能从任意调用线程发起取消，切回网络 loop 处理状态
    std::weak_ptr<MprpcChannel> weak_this = shared_from_this();
    loop_->queueInLoop([weak_this, req_id]()
                       {
                           if (auto self = weak_this.lock())
                           {
                               self->cancelInLoop(req_id);
                           } });
}

void MprpcChannel::cancelInLoop(uint64_t req_id)
{
    // 删除 pending 项后再运行 done，避免回调重入时重复完成
    std::shared_ptr<PendingCall> call = takePendingCall(req_id);
    if (!call)
    {
        return;
    }

    loop_->cancel(call->timerId);
    sendCancelInLoop(req_id, call);

    if (call->controller)
    {
        call->controller->SetFailed("rpc canceled");
    }
    if (call->done)
    {
        call->done->Run();
    }
}

void MprpcChannel::onTimeout(uint64_t req_id)
{
    // 定时器与响应在同一 loop 线程竞争，只有先取出 pending 的一方完成调用
    std::shared_ptr<PendingCall> call = takePendingCall(req_id);
    if (!call)
    {
        return;
    }

    sendCancelInLoop(req_id, call);

    if (call->controller)
    {
        call->controller->SetFailed("rpc timeout");
    }
    if (call->done)
    {
        call->done->Run();
    }
}

size_t MprpcChannel::pendingCallCount() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_pendingCalls.size();
}

class DiscoveryDone : public google::protobuf::Closure
{
public:
    DiscoveryDone(const std::shared_ptr<MprpcDiscoveryChannel> &owner,
                  google::protobuf::Closure *done)
        : owner_(owner), done_(done)
    {
    }

    void Run() override
    {
        std::shared_ptr<MprpcDiscoveryChannel> owner = owner_.lock();
        google::protobuf::Closure *done = done_;
        delete this;
        if (done)
        {
            done->Run();
        }
        if (owner)
        {
            owner->SchedulePrune();
        }
    }

private:
    std::weak_ptr<MprpcDiscoveryChannel> owner_;
    google::protobuf::Closure *done_;
};

MprpcDiscoveryChannel::MprpcDiscoveryChannel(
    EventLoop *loop, const std::string &serviceName, double zkConnectTimeoutSec)
    : loop_(loop),
      serviceName_(serviceName),
      zkConnectTimeoutSec_(zkConnectTimeoutSec)
{
    if (!loop_ || serviceName_.empty() ||
        !std::isfinite(zkConnectTimeoutSec_) || zkConnectTimeoutSec_ <= 0.0)
    {
        throw std::invalid_argument("invalid discovery channel configuration");
    }
    // Start 有界等待，若超时，首次调用时还会重试连接
    zkReady_ = ZkClient::global().Start(zkConnectTimeoutSec_);
    if (!zkReady_)
    {
        LOG_ERROR("discovery channel for %s created without ZooKeeper connection",
                  serviceName_.c_str());
    }
}

MprpcDiscoveryChannel::~MprpcDiscoveryChannel()
{
    for (const std::string &path : watchedPaths_)
    {
        ZkClient::global().removeWatch(path.c_str(), this);
    }
}

bool MprpcDiscoveryChannel::ParseEndpoint(const std::string &endpoint,
                                          InetAddress *address) const
{
    const size_t separator = endpoint.rfind(':');
    if (!address || separator == std::string::npos || separator == 0 ||
        separator + 1 >= endpoint.size())
    {
        return false;
    }

    const std::string host = endpoint.substr(0, separator);
    const std::string portText = endpoint.substr(separator + 1);
    // mymuduo 的 InetAddress 使用 IPv4 sockaddr_in，拒绝无法路由的节点值
    in_addr parsedAddress;
    if (inet_pton(AF_INET, host.c_str(), &parsedAddress) != 1 ||
        !std::all_of(portText.begin(), portText.end(),
                     [](unsigned char digit)
                     { return digit >= '0' && digit <= '9'; }))
    {
        return false;
    }

    char *end = nullptr;
    const long port = std::strtol(portText.c_str(), &end, 10);
    if (end == portText.c_str() || *end != '\0' || port < 1 || port > 65535)
    {
        return false;
    }

    *address = InetAddress(static_cast<uint16_t>(port), host);
    return true;
}

void MprpcDiscoveryChannel::ApplyEndpointUpdate(
    const std::string &methodKey, const std::vector<std::string> &children)
{
    std::vector<std::string> endpoints;
    for (const std::string &child : children)
    {
        InetAddress address;
        if (ParseEndpoint(child, &address))
        {
            endpoints.push_back(child);
        }
        else
        {
            LOG_ERROR("ignoring invalid service endpoint: %s", child.c_str());
        }
    }
    std::sort(endpoints.begin(), endpoints.end());
    endpoints.erase(std::unique(endpoints.begin(), endpoints.end()), endpoints.end());

    // 固定排序使同一组节点在各客户端上的轮询顺序稳定
    {
        std::lock_guard<std::mutex> lock(discoveryMutex_);
        methodEndpoints_[methodKey] = std::move(endpoints);
    }
    SchedulePrune();
}

void MprpcDiscoveryChannel::RefreshMethod(const std::string &methodKey,
                                          const std::string &path)
{
    std::vector<std::string> children;
    std::weak_ptr<MprpcDiscoveryChannel> weakThis = shared_from_this();
    // 读取最新实例并重新安装一次性 watch
    const bool found = ZkClient::global().GetChildrenData(
        path.c_str(), children,
        [weakThis, methodKey, path]()
        {
            if (std::shared_ptr<MprpcDiscoveryChannel> self = weakThis.lock())
            {
                self->RefreshMethod(methodKey, path);
            }
        },
        this);
    if (found)
    {
        ApplyEndpointUpdate(methodKey, children);
    }
    else
    {
        ApplyEndpointUpdate(methodKey, std::vector<std::string>());
    }
}

void MprpcDiscoveryChannel::CallMethod(
    const google::protobuf::MethodDescriptor *method,
    google::protobuf::RpcController *controller,
    const google::protobuf::Message *request,
    google::protobuf::Message *response,
    google::protobuf::Closure *done)
{
    auto failCall = [controller, done](const std::string &message)
    {
        if (controller)
        {
            controller->SetFailed(message);
        }
        if (done)
        {
            done->Run();
        }
    };

    if (!method || !request || !response ||
        method->service()->name() != serviceName_)
    {
        failCall("invalid RPC method or message for discovery channel");
        return;
    }

    std::shared_ptr<MprpcDiscoveryChannel> self;
    try
    {
        self = shared_from_this();
    }
    catch (const std::bad_weak_ptr &)
    {
        failCall("MprpcDiscoveryChannel must be owned by std::shared_ptr");
        return;
    }

    bool zkReady = false;
    {
        std::lock_guard<std::mutex> lock(discoveryMutex_);
        zkReady = zkReady_;
    }
    if (!zkReady && !ZkClient::global().Start(zkConnectTimeoutSec_))
    {
        failCall("zookeeper unavailable for service discovery");
        return;
    }
    {
        std::lock_guard<std::mutex> lock(discoveryMutex_);
        zkReady_ = true;
    }

    const std::string methodName = method->name();
    const std::string methodKey = serviceName_ + "." + methodName;
    const std::string path = "/" + serviceName_ + "/" + methodName;
    bool needWatch = false;
    {
        std::lock_guard<std::mutex> lock(discoveryMutex_);
        needWatch = watchedPaths_.insert(path).second;
    }
    if (needWatch)
    {
        // 每个 service/method 首次调用时建立订阅，并立即读取现有实例
        std::vector<std::string> children;
        std::weak_ptr<MprpcDiscoveryChannel> weakThis = self;
        const bool found = ZkClient::global().GetChildrenData(
            path.c_str(), children,
            [weakThis, methodKey, path]()
            {
                if (std::shared_ptr<MprpcDiscoveryChannel> channel = weakThis.lock())
                {
                    channel->RefreshMethod(methodKey, path);
                }
            },
            this);
        ApplyEndpointUpdate(methodKey,
                            found ? children : std::vector<std::string>());
    }

    PruneRetiredClients();
    std::shared_ptr<MprpcChannel> selectedClient;
    std::string selectedEndpoint;
    std::string selectionError;
    {
        std::lock_guard<std::mutex> lock(discoveryMutex_);
        auto endpoints = methodEndpoints_.find(methodKey);
        if (endpoints == methodEndpoints_.end() || endpoints->second.empty())
        {
            selectionError = "no available instances for " + methodKey;
        }
        else
        {
            // 每个方法独立轮询实例，其余请求复用对应 endpoint 的连接
            size_t &next = nextEndpoint_[methodKey];
            selectedEndpoint = endpoints->second[next % endpoints->second.size()];
            next = (next + 1) % endpoints->second.size();

            auto client = clients_.find(selectedEndpoint);
            if (client == clients_.end())
            {
                InetAddress address;
                if (!ParseEndpoint(selectedEndpoint, &address))
                {
                    selectionError = "invalid discovered endpoint: " + selectedEndpoint;
                }
                else
                {
                    selectedClient.reset(new MprpcChannel(loop_, address));
                    std::weak_ptr<MprpcDiscoveryChannel> weakSelf = self;
                    selectedClient->disconnectedCallback_ =
                        [weakSelf]()
                    {
                        if (std::shared_ptr<MprpcDiscoveryChannel> owner =
                                weakSelf.lock())
                        {
                            owner->SchedulePrune();
                        }
                    };
                    clients_.emplace(selectedEndpoint, selectedClient);
                }
            }
            else
            {
                selectedClient = client->second;
            }
        }
    }
    if (!selectionError.empty())
    {
        failCall(selectionError);
        return;
    }

    google::protobuf::Closure *completion = new DiscoveryDone(self, done);
    selectedClient->CallMethod(method, controller, request, response, completion);
}

void MprpcDiscoveryChannel::SchedulePrune()
{
    std::shared_ptr<MprpcDiscoveryChannel> self = shared_from_this();
    loop_->queueInLoop([self]()
                       { self->PruneRetiredClients(); });
}

void MprpcDiscoveryChannel::PruneRetiredClients()
{
    std::lock_guard<std::mutex> lock(discoveryMutex_);
    for (auto client = clients_.begin(); client != clients_.end();)
    {
        bool active = false;
        for (const auto &method : methodEndpoints_)
        {
            if (std::find(method.second.begin(), method.second.end(),
                          client->first) != method.second.end())
            {
                active = true;
                break;
            }
        }
        if (!active && client->second->pendingCallCount() == 0)
        {
            // 先停用连接，等断开回调后再销毁 TcpClient，避免晚到回调访问已释放对象
            if (client->second->client_.connection())
            {
                client->second->client_.disconnect();
                ++client;
            }
            else
            {
                client = clients_.erase(client);
            }
        }
        else
        {
            ++client;
        }
    }
}

void MprpcChannel::sendCancelInLoop(
    uint64_t req_id, const std::shared_ptr<PendingCall> &call)
{
    // 仅已发送的请求需要通知服务端，尚在本地队列中的请求可直接丢弃
    if (!call->sent)
    {
        return;
    }

    TcpConnectionPtr conn = call->connection.lock();
    if (!conn || !conn->connected())
    {
        return;
    }

    RpcFrameHeader header;
    header.set_req_id(req_id);
    header.set_control(kRpcFrameControlCancel);
    header.set_args_size(0);
    header.set_raw_size(0);

    std::string packet;
    if (BeginEncodeRpcFrame(header, 0, &packet) != kRpcFrameInvalidOffset)
    {
        conn->send(packet);
    }
}