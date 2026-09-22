#pragma once
#include "google/protobuf/service.h"
#include <mymuduo/TcpServer.h>
#include <mymuduo/EventLoop.h>
#include <mymuduo/InetAddress.h>
#include <mymuduo/TcpConnection.h>
#include <string>
#include <functional>
#include <google/protobuf/descriptor.h>
#include <unordered_map>

class RpcProvider
{
public:
    void NotifyService(google::protobuf::Service *service);

    void Run();

private:
    // 组合了EventLoop
    EventLoop m_eventLoop;

    // service服务类型信息
    struct ServiceInfo
    {
        // 保存服务对象
        google::protobuf::Service *m_service;
        // 保存服务方法
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;
    };

    // 存储注册成功的服务对象和其服务方法的所有信息
    std::unordered_map<std::string, ServiceInfo> m_serviceMap;

    // 新的socket连接回调
    void OnConnection(const TcpConnectionPtr &);

    // 已建立连接用户的读写事件回调
    void OnMessage(const TcpConnectionPtr &, Buffer *, Timestamp);

    // Closure的回调操作，用于序列化rpc的响应和网络发送
    void SendrpcResponse(const TcpConnectionPtr &, google::protobuf::Message *);
};
