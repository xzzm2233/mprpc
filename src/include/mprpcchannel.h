#pragma once
#include "rpcheader.pb.h"

#include <mymuduo/TcpClient.h>
#include <mymuduo/EventLoop.h>
#include <mymuduo/InetAddress.h>
#include <mymuduo/TcpConnection.h>
#include <mymuduo/Buffer.h>
#include <mymuduo/Timestamp.h>

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <memory>
#include <string>
#include <atomic>
#include <mutex>
#include <unordered_map>

class MprpcController;

class MprpcChannel : public google::protobuf::RpcChannel
{
public:
    MprpcChannel(EventLoop *loop, const InetAddress &serverAddr);
    ~MprpcChannel();

    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done);

private:
    void onConnection(const TcpConnectionPtr &conn);
    void onMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time);
    void onResponse(const mprpc::RpcHeader &header, const std::string &body);

    void onCancelCallback(uint64_t req_id);
    void cancelInLoop(uint64_t req_id);

    struct PendingCall
    {
        google::protobuf::RpcController *controller;
        google::protobuf::Message *response;
        google::protobuf::Closure *done;
    };

    EventLoop *loop_;
    TcpClient client_;
    TcpConnectionPtr connection_;

    std::atomic<uint64_t> m_reqId;
    std::mutex m_mutex;
    std::unordered_map<uint64_t, PendingCall> m_pendingCalls;
};