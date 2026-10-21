#pragma once
#include <google/protobuf/service.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>

class MprpcController : public google::protobuf::RpcController
{
public:
    // Protobuf RPC 控制器扩展：承载错误、取消、超时预算和 trace ID。
    MprpcController();
    ~MprpcController();

    // 清空上一次调用的状态，以便安全复用控制器。
    void Reset();
    bool Failed() const;
    std::string ErrorText() const;
    void SetFailed(const std::string &reason);

    void StartCancel();
    bool IsCanceled() const;

    // 安装由 StartCancel() 触发的通道回调。
    void setCancelCallback(std::function<void()> cb);
    void NotifyOnCancel(google::protobuf::Closure *callback) override;

    // ---- 客户端侧：本次调用的超时预算（毫秒），0 表示使用 channel 默认值 ----
    void SetRpcTimeoutMs(int64_t ms);
    int64_t RpcTimeoutMs() const;

    // ---- 服务端侧：上游透传过来的 deadline ----
    // 收到请求时调用：记录剩余预算与起始时刻
    void SetDeadline(uint32_t deadlineMs);
    // 剩余预算（毫秒）。返回 <0 表示从未设置 deadline（无限制）；
    // 已经超时则返回 0。
    int64_t RemainingMs() const;
    bool HasDeadline() const { return deadlineMs_.load(std::memory_order_relaxed) > 0; }

    // ---- 链路追踪 ----
    void setTraceId(uint64_t id) { traceId_.store(id, std::memory_order_relaxed); }
    uint64_t traceId() const { return traceId_.load(std::memory_order_relaxed); }

    // ---- 重试语义：该方法是否幂等（连接中断时是否允许重发）----
    void SetIdempotent(bool v) { idempotent_.store(v, std::memory_order_relaxed); }
    bool Idempotent() const { return idempotent_.load(std::memory_order_relaxed); }

private:
    std::atomic<bool> m_failed;
    std::atomic<bool> m_canceled;
    mutable std::mutex mu_; // 保护 m_errText / m_cancelCallback
    std::string m_errText;
    std::function<void()> m_cancelCallback;

    std::atomic<int64_t> rpcTimeoutMs_{0}; // 客户端超时配置，0 表示通道默认值
    std::atomic<int64_t> deadlineMs_{0}; // 0 = 无 deadline
    std::atomic<int64_t> deadlineStartNs_{0};
    std::atomic<uint64_t> traceId_{0};
    std::atomic<bool> idempotent_{false};

    static int64_t steadyNowNs();
};
