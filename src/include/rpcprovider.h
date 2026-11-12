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
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class MprpcController;
struct RpcActiveCall;

// 连接级准入状态，跨线程共享字段使用原子变量
struct RpcConnStats
{
    std::atomic<int> inflight{0};         // 当前连接正在处理的请求数
    std::atomic<size_t> inflightBytes{0}; // 当前连接已预留的请求体字节数
    std::atomic<bool> overloaded{false};  // 是否因输出背压暂时拒绝新请求
    int highWaterHits = 0;                // 当前连接触发的高水位次数，仅所属 loop 线程访问
    std::mutex activeMu;                  // 保护活动请求表
    // 按请求 ID 查找可取消调用，弱引用不延长已完成调用的生命周期
    std::unordered_map<uint64_t, std::weak_ptr<RpcActiveCall>> activeCalls;
};

class RpcProvider
{
public:
    // 注册一个 protobuf 服务，服务对象必须比 provider 活得更久
    // 同一服务可能被多个工作线程并发调用，服务实现必须保证线程安全
    void NotifyService(google::protobuf::Service *service);

    // 读取运行配置、启动监听/工作线程并进入 IO 事件循环
    void Run();

private:
    struct ServiceInfo
    {
        std::string name;                             // protobuf 服务名称
        google::protobuf::Service *service = nullptr; // 注册的 protobuf 服务实例
        // 由服务描述符构建，供请求分发时按方法名查找
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> methods;
    };

    // 一次业务调用的上下文：由 done 闭包负责回收
    struct RpcContext
    {
        TcpConnectionPtr conn;                     // 承载本次 RPC 的客户端连接
        uint64_t reqId = 0;                        // 本次请求的唯一标识
        uint64_t traceId = 0;                      // 本次调用的追踪标识
        std::shared_ptr<RpcConnStats> stats;       // 所属连接的准入统计状态
        std::shared_ptr<RpcActiveCall> activeCall; // 支持协作取消的活动调用状态
        size_t accountedBytes = 0;                 // 本次请求预留的 wire body 与解压 body 字节数

        std::string methodKey;  // "service.method"，指标用
        int64_t startNs = 0;    // 帧首字节到达时刻
        int64_t deadlineNs = 0; // 0 = 无 deadline（steady ns）

        std::unique_ptr<google::protobuf::Message> request;  // 反序列化后的请求消息
        std::unique_ptr<google::protobuf::Message> response; // 服务生成的响应消息
        std::shared_ptr<MprpcController> controller;         // 保存调用状态并提供协作取消接口

        bool rejected = false; // 队列拒绝时抑制重复响应
    };

    // 连接生命周期、请求读取和输出背压回调均由 IO loop 调用
    void OnConnection(const TcpConnectionPtr &conn);
    void OnMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time);
    void OnHighWater(const TcpConnectionPtr &conn, size_t mark);
    void OnWriteComplete(const TcpConnectionPtr &conn);
    // 按连接状态和请求 ID 查找活动调用，未知或已完成的请求会被忽略
    void CancelRequest(const std::shared_ptr<RpcConnStats> &stats,
                       uint64_t reqId);

    // 在 IO 线程完成帧级工作后校验并投递到业务线程池
    void ProcessRequest(const TcpConnectionPtr &conn,
                        const std::shared_ptr<RpcConnStats> &stats,
                        const RpcFrame &frame,
                        const std::string &args,
                        int64_t startNs);

    // 业务 done 回调：记录结果、发送响应并回收调用上下文
    void OnResponse(RpcContext *context);

    // 响应统一使用 protobuf 帧头，大响应可按配置尝试压缩
    void SendResponse(const TcpConnectionPtr &conn, uint64_t reqId, uint64_t traceId,
                      google::protobuf::Message *response);
    void SendError(const TcpConnectionPtr &conn, uint64_t reqId, uint64_t traceId,
                   int32_t errCode, const std::string &errMsg);

    void DumpMetrics();

    EventLoop m_eventLoop;                    // provider 的主 IO loop
    std::unique_ptr<RpcThreadPool> m_workers; // 执行业务服务方法的工作线程池

    // 服务注册完成后只读，运行期间由 IO/工作线程共享
    std::unordered_map<std::string, ServiceInfo> m_serviceMap;

    // 运行期配置（Run() 里读取），字节预算按 wire body + 解压 body 估算
    int m_ioThreads = 4;      // IO 事件循环线程数
    int m_maxInflight = 1024; // 每个连接允许的最大并发请求数
    // 全局并发与请求 payload 上限，避免大量连接绕过连接级限流
    int m_maxGlobalInflight = 8192;                       // 全局允许的最大并发请求数
    size_t m_maxGlobalInflightBytes = 512u * 1024 * 1024; // 全局请求体字节预算上限
    size_t m_maxConnInflightBytes = 128u * 1024 * 1024;   // 单连接请求体字节预算上限
    std::atomic<int> m_globalInflight{0};                 // 当前全局预留的并发请求数
    std::atomic<size_t> m_globalInflightBytes{0};         // 当前全局预留的请求体字节数
    size_t m_highWater = 8u * 1024 * 1024;                // 连接输出缓冲区的高水位阈值
    bool m_compress = true;                               // 是否启用响应压缩
    int m_compressLevel = 3;                              // Zstandard 响应压缩级别
    size_t m_compressMin = 8 * 1024;                      // 尝试压缩的最小响应字节数
    int m_metricsInterval = 10;                           // 指标输出间隔秒数，0 表示关闭
};
