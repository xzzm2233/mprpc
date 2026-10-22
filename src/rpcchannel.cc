#include "rpcchannel.h"
#include "rpccontroller.h"

#include <mymuduo/Logger.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <utility>

static const int64_t kMprpcChannelDefaultTimeoutMs = 30000;

// MprpcChannel 通过 RpcChannel 提供异步请求/响应映射
MprpcChannel::MprpcChannel(EventLoop *loop, const InetAddress &serverAddr)
    : loop_(loop),
      client_(loop, serverAddr, "MprpcChannel"),
      m_reqId(1)
{
    client_.setConnectionCallback(std::bind(&MprpcChannel::onConnection, this,
                                            std::placeholders::_1));
    client_.setMessageCallback(std::bind(&MprpcChannel::onMessage, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2,
                                         std::placeholders::_3));
    client_.enableRetry();
    client_.connect();
}

MprpcChannel::~MprpcChannel()
{
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
    if (auto *ctrl = dynamic_cast<MprpcController *>(controller))
    {
        if (ctrl->RpcTimeoutMs() > 0)
        {
            timeoutMs = ctrl->RpcTimeoutMs();
        }
        if (ctrl->HasDeadline())
        {
            const int64_t remainingMs = ctrl->RemainingMs();
            if (remainingMs <= 0)
            {
                ctrl->SetFailed("rpc deadline exceeded");
                if (done)
                {
                    done->Run();
                }
                return;
            }
            timeoutMs = std::min(timeoutMs, remainingMs);
        }
        header.set_trace_id(ctrl->traceId());
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

    auto call = std::make_shared<PendingCall>();
    call->controller = controller;
    call->response = response;
    call->done = done;

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pendingCalls[req_id] = call;
    }

    if (auto *ctrl = dynamic_cast<MprpcController *>(controller))
    {
        ctrl->setCancelCallback(
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
        auto it = m_pendingCalls.find(req_id);
        if (it != m_pendingCalls.end() && it->second == call)
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

    TcpConnectionPtr conn = client_.connection();
    loop_->queueInLoop([weak_this, req_id, conn, packet]()
                       {
                           if (auto self = weak_this.lock())
                           {
                               self->sendRequestInLoop(req_id, conn, packet);
                           }
                       });
}

void MprpcChannel::onConnection(const TcpConnectionPtr &conn)
{
    // 断线时所有在途请求都无法再收到响应，统一取消定时器并通知调用方
    if (conn->connected())
    {
        responseFrame_.reset();
        LOG_INFO("MprpcChannel connected to %s", conn->peerAddress().toIpPort().c_str());
    }
    else
    {
        responseFrame_.reset();
        LOG_INFO("MprpcChannel disconnected from %s",
                 conn->peerAddress().toIpPort().c_str());

        std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            pending.swap(m_pendingCalls);
        }
        for (auto &kv : pending)
        {
            loop_->cancel(kv.second->timerId);
            if (kv.second->controller)
            {
                kv.second->controller->SetFailed("connection closed");
            }
            if (kv.second->done)
            {
                kv.second->done->Run();
            }
        }
    }
}
void MprpcChannel::onMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time)
{
    // 响应按帧增量解析；完整帧可能一次收到多条
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
    // req_id 将响应关联到原始调用；查找不到时说明调用已超时或取消
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
    auto it = m_pendingCalls.find(req_id);
    if (it == m_pendingCalls.end())
    {
        return std::shared_ptr<PendingCall>();
    }

    std::shared_ptr<PendingCall> call = it->second;
    m_pendingCalls.erase(it);
    return call;
}

void MprpcChannel::sendRequestInLoop(uint64_t req_id,
                                     const TcpConnectionPtr &conn,
                                     const std::string &packet)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_pendingCalls.find(req_id) == m_pendingCalls.end())
        {
            return;
        }
    }

    if (conn && conn->connected())
    {
        conn->send(packet);
        return;
    }

    std::shared_ptr<PendingCall> call = takePendingCall(req_id);
    if (!call)
    {
        return;
    }
    loop_->cancel(call->timerId);
    if (call->controller)
    {
        call->controller->SetFailed("connection not ready");
    }
    if (call->done)
    {
        call->done->Run();
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
                           }
                       });
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

    if (call->controller)
    {
        call->controller->SetFailed("rpc timeout");
    }
    if (call->done)
    {
        call->done->Run();
    }
}