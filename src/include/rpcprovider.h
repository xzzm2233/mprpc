#pragma once
#include "rpcframe.h"
#include "rpcthreadpool.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <mymuduo/EventLoop.h>
#include <mymuduo/InetAddress.h>
#include <mymuduo/TcpConnection.h>
#include <mymuduo/TcpServer.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// 连接级准入状态；跨线程共享字段使用原子变量
struct RpcConnStats
{
    std::atomic<int> inflight{0};
    std::atomic<bool> overloaded{false};
    int highWaterHits = 0; // 仅所属 loop 线程访问
};

class MprpcController;

class RpcProvider
{
public:
    // 注册一个 protobuf 服务；服务对象必须比 provider 活得更久
    void NotifyService(google::protobuf::Service *service);

    // 读取运行配置、启动监听/工作线程并进入 IO 事件循环
    void Run();

private:
    struct ServiceInfo
    {
        std::string name;
        google::protobuf::Service *service = nullptr;
        // 由服务描述符构建，供请求分发时按方法名查找
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> methods;
    };

    // 一次业务调用的上下文：由 done 闭包负责回收
    struct RpcContext
    {
        TcpConnectionPtr conn;
        uint64_t reqId = 0;
        uint64_t traceId = 0;
        std::shared_ptr<RpcConnStats> stats;

        std::string methodKey; // "service.method"，指标用
        int64_t startNs = 0;   // 帧首字节到达时刻
        int64_t deadlineNs = 0; // 0 = 无 deadline（steady ns）

        std::unique_ptr<google::protobuf::Message> request;
        std::unique_ptr<google::protobuf::Message> response;
        std::unique_ptr<MprpcController> controller;

        bool rejected = false; // 队列拒绝时抑制重复响应
    };

    // 连接生命周期、请求读取和输出背压回调均由 IO loop 调用。
    void OnConnection(const TcpConnectionPtr &conn);
    void OnMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time);
    void OnHighWater(const TcpConnectionPtr &conn, size_t mark);
    void OnWriteComplete(const TcpConnectionPtr &conn);

    // 在 IO 线程完成帧级工作后校验并投递到业务线程池
    void ProcessRequest(const TcpConnectionPtr &conn,
                        const std::shared_ptr<RpcConnStats> &stats,
                        const RpcFrame &frame,
                        const std::string &args,
                        int64_t startNs);

    // 业务 done 回调：记录结果、发送响应并回收调用上下文
    void OnResponse(RpcContext *ctx);

    // 响应统一使用 protobuf 帧头；大响应可按配置尝试压缩
    void SendResponse(const TcpConnectionPtr &conn, uint64_t reqId, uint64_t traceId,
                      google::protobuf::Message *response);
    void SendError(const TcpConnectionPtr &conn, uint64_t reqId, uint64_t traceId,
                   int32_t errCode, const std::string &errMsg);

    void DumpMetrics();

    EventLoop m_eventLoop; // provider 的主 IO loop
    std::unique_ptr<RpcThreadPool> m_workers;

    // 服务注册完成后只读，运行期间由 IO/工作线程共享
    std::unordered_map<std::string, ServiceInfo> m_serviceMap;

    // 运行期配置（Run() 里读取）
    int m_ioThreads = 4;
    int m_maxInflight = 1024;
    size_t m_highWater = 8u * 1024 * 1024;
    bool m_compress = true;
    int m_compressLevel = 3;
    size_t m_compressMin = 8 * 1024;
    int m_metricsInterval = 10;
};
