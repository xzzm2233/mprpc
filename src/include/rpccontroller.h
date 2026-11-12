#pragma once
#include <google/protobuf/service.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

class MprpcController : public google::protobuf::RpcController
{
public:
    // Protobuf RPC 控制器扩展：承载错误、取消、超时预算和 trace ID
    MprpcController();
    ~MprpcController();

    // 清空上一次调用的状态，以便安全复用控制器
    void Reset();
    // 查询当前调用是否失败，并读取或设置对应错误信息
    bool Failed() const;
    std::string ErrorText() const;
    void SetFailed(const std::string &reason);

    // 发起一次协作式取消，已注册回调在调用 StartCancel 的线程执行
    void StartCancel();
    // 查询当前调用是否已请求取消
    bool IsCanceled() const;

    // 客户端取消会通知服务端，服务端 handler 应检查 IsCanceled() 或注册回调，
    // 取消是协作式的，不会强制终止正在运行的业务线程
    // 回调在触发取消的线程执行，若注册时已取消则在注册线程立即执行
    void setCancelCallback(std::function<void()> cb);
    void NotifyOnCancel(google::protobuf::Closure *callback) override;

    // ---- 客户端侧：本次调用的超时预算（毫秒），0 表示使用 channel 默认值 ----
    void SetRpcTimeoutMs(int64_t ms);
    int64_t RpcTimeoutMs() const;

    // ---- 服务端侧：上游透传过来的 deadline ----
    // 收到请求时调用：记录剩余预算与起始时刻
    void SetDeadline(uint32_t deadlineMs);
    // 剩余预算（毫秒），返回 <0 表示从未设置 deadline（无限制）
    // 已经超时则返回 0
    int64_t RemainingMs() const;
    bool HasDeadline() const { return deadlineMs_.load(std::memory_order_relaxed) > 0; }

    void setTraceId(uint64_t id) { traceId_.store(id, std::memory_order_relaxed); }
    uint64_t traceId() const { return traceId_.load(std::memory_order_relaxed); }

    // 保存调用幂等标志，该标志不会触发框架自动重试
    void SetIdempotent(bool idempotent)
    {
        idempotent_.store(idempotent, std::memory_order_relaxed);
    }
    bool Idempotent() const { return idempotent_.load(std::memory_order_relaxed); }

private:
    std::atomic<bool> m_failed;
    std::atomic<bool> m_canceled;
    mutable std::mutex mu_; // 保护 m_errText / m_cancelCallback
    std::string m_errText;
    std::function<void()> m_cancelCallback;
    std::vector<google::protobuf::Closure *> m_cancelNotifications;

    std::atomic<int64_t> rpcTimeoutMs_{0}; // 客户端超时配置，0 表示通道默认值
    std::atomic<int64_t> deadlineMs_{0};
    std::atomic<int64_t> deadlineStartNs_{0};
    std::atomic<uint64_t> traceId_{0};
    std::atomic<bool> idempotent_{false};

    static int64_t steadyNowNs();
};
