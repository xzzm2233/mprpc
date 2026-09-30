#pragma once
#include "rpcheader.pb.h"

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <mymuduo/TcpServer.h>
#include <mymuduo/EventLoop.h>
#include <mymuduo/InetAddress.h>
#include <mymuduo/TcpConnection.h>
#include <string>
#include <memory>
#include <functional>
#include <unordered_map>

class MprpcController;

class RpcProvider
{
public:
    void NotifyService(google::protobuf::Service *service);

    void Run();

private:
    // 组合了EventLoop
    EventLoop m_eventLoop;

    struct ServiceInfo
    {
        // 保存服务对象
        google::protobuf::Service *m_service;
        // 保存服务方法
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;
    };

    std::unordered_map<std::string, ServiceInfo> m_serviceMap;

    void OnConnection(const TcpConnectionPtr &conn);

    void OnMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time);

    void SendResponse(const TcpConnectionPtr &conn,
                      uint64_t req_id,
                      google::protobuf::Message *response);

    void ProcessRequest(const TcpConnectionPtr &conn,
                        const mprpc::RpcHeader &header,
                        const std::string &args_str);

    void SendError(const TcpConnectionPtr &conn,
                   uint64_t req_id,
                   int32_t err_code,
                   const std::string &err_msg);

    struct RpcContext
    {
        TcpConnectionPtr conn;
        uint64_t req_id;
        std::shared_ptr<google::protobuf::Message> response;
        std::shared_ptr<google::protobuf::Message> request;
        std::shared_ptr<MprpcController> controller;
    };

    void OnResponse(RpcContext *ctx);
};
