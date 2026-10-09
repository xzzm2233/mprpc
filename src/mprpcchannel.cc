#include "mprpcchannel.h"
#include "mprpccontroller.h"

#include <mymuduo/Logger.h>

#include <cstring>
#include <functional>
#include <arpa/inet.h>

namespace
{
    // 网络报文长度上限，防止恶意/异常报文导致 OOM
    uint32_t kMaxHeaderSize = 1 * 1024 * 1024; // 1MB
    uint32_t kMaxBodySize = 64 * 1024 * 1024;  // 64MB
    // 单个 RPC 请求的超时时间（秒）
    double kTimeoutSeconds = 30.0;
}

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
    uint64_t req_id = m_reqId++;

    mprpc::RpcHeader header;
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
    header.set_args_size(static_cast<uint32_t>(args.size()));

    std::string header_str;
    header.SerializeToString(&header_str);
    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t header_size_be = htonl(header_size);

    std::string packet;
    packet.append(reinterpret_cast<char *>(&header_size_be), 4);
    packet.append(header_str);
    packet.append(args);

    if (auto *ctrl = dynamic_cast<MprpcController *>(controller))
    {
        ctrl->setCancelCallback(
            std::bind(&MprpcChannel::onCancelCallback, this, req_id));
    }

    // 注册超时定时器
    TimerId timer = loop_->runAfter(
        kTimeoutSeconds,
        std::bind(&MprpcChannel::onTimeout, this, req_id));

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pendingCalls[req_id] = {controller, response, done, timer};
    }

    TcpConnectionPtr conn = client_.connection();
    if (conn && conn->connected())
    {
        conn->send(packet);
    }
    else
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_pendingCalls.erase(req_id);
        }
        loop_->cancel(timer);
        if (controller)
        {
            controller->SetFailed("connection not ready");
        }
        if (done)
        {
            done->Run();
        }
    }
}

void MprpcChannel::onConnection(const TcpConnectionPtr &conn)
{
    if (conn->connected())
    {
        LOG_INFO("MprpcChannel connected to %s", conn->peerAddress().toIpPort().c_str());
    }
    else
    {
        LOG_INFO("MprpcChannel disconnected from %s",
                 conn->peerAddress().toIpPort().c_str());

        std::unordered_map<uint64_t, PendingCall> pending;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            pending.swap(m_pendingCalls);
        }
        for (auto &kv : pending)
        {
            loop_->cancel(kv.second.timerId);
            if (kv.second.controller)
            {
                kv.second.controller->SetFailed("connection closed");
            }
            if (kv.second.done)
            {
                kv.second.done->Run();
            }
        }
    }
}
void MprpcChannel::onMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time)
{
    (void)conn;

    while (true)
    {
        if (buffer->readableBytes() < 4)
        {
            return;
        }

        uint32_t header_size_be = 0;
        ::memcpy(&header_size_be, buffer->peek(), 4);
        uint32_t header_size = ntohl(header_size_be);
        if (header_size > kMaxHeaderSize)
        {
            LOG_ERROR("header_size too large: %u", header_size);
            buffer->retrieveAll();
            return;
        }

        if (buffer->readableBytes() < 4 + header_size)
        {
            return;
        }

        std::string header_str(buffer->peek() + 4, header_size);
        mprpc::RpcHeader header;
        if (!header.ParseFromString(header_str))
        {
            LOG_ERROR("response header parse error");
            buffer->retrieveAll();
            return;
        }

        uint32_t args_size = header.args_size();
        if (args_size > kMaxBodySize)
        {
            LOG_ERROR("args_size too large: %u", args_size);
            buffer->retrieveAll();
            return;
        }
        uint32_t total = 4 + header_size + args_size;

        if (buffer->readableBytes() < total)
        {
            return;
        }

        std::string body(buffer->peek() + 4 + header_size, args_size);

        buffer->retrieve(total);

        onResponse(header, body);
    }
}
void MprpcChannel::onResponse(const mprpc::RpcHeader &header, const std::string &body)
{
    uint64_t req_id = header.req_id();

    PendingCall call;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_pendingCalls.find(req_id);
        if (it == m_pendingCalls.end())
        {
            LOG_INFO("no pending call for req_id=%lu", (unsigned long)req_id);
            return;
        }
        call = it->second;
        m_pendingCalls.erase(it);
    }

    loop_->cancel(call.timerId);

    if (header.err_code() != 0)
    {
        if (call.controller)
            call.controller->SetFailed(header.err_msg());
    }
    else
    {
        if (!call.response->ParseFromString(body))
        {
            if (call.controller)
                call.controller->SetFailed("parse response error");
        }
    }

    if (call.done)
    {
        call.done->Run();
    }
}

void MprpcChannel::onCancelCallback(uint64_t req_id)
{
    loop_->queueInLoop(
        std::bind(&MprpcChannel::cancelInLoop, this, req_id));
}

void MprpcChannel::cancelInLoop(uint64_t req_id)
{
    PendingCall call;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_pendingCalls.find(req_id);
        if (it == m_pendingCalls.end())
        {
            return;
        }
        call = it->second;
        m_pendingCalls.erase(it);
    }

    loop_->cancel(call.timerId);

    if (call.controller)
    {
        call.controller->SetFailed("rpc canceled");
    }
    if (call.done)
    {
        call.done->Run();
    }
}

void MprpcChannel::onTimeout(uint64_t req_id)
{
    PendingCall call;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_pendingCalls.find(req_id);
        if (it == m_pendingCalls.end())
        {
            return;
        }
        call = it->second;
        m_pendingCalls.erase(it);
    }

    if (call.controller)
    {
        call.controller->SetFailed("rpc timeout");
    }
    if (call.done)
    {
        call.done->Run();
    }
}