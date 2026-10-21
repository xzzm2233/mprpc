#include "rpccontroller.h"

#include <chrono>

// deadline 计算必须使用单调时钟，避免系统时间校准影响剩余预算。
int64_t MprpcController::steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

MprpcController::MprpcController()
    : m_failed(false),
      m_canceled(false)
{
}

MprpcController::~MprpcController()
{
}

void MprpcController::Reset()
{
    // 清理状态和互斥量保护的数据，允许调用方复用 controller。
    m_failed.store(false, std::memory_order_relaxed);
    m_canceled.store(false, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(mu_);
        m_errText.clear();
        m_cancelCallback = nullptr;
    }
    rpcTimeoutMs_.store(0, std::memory_order_relaxed);
    deadlineMs_.store(0, std::memory_order_relaxed);
    deadlineStartNs_.store(0, std::memory_order_relaxed);
    traceId_.store(0, std::memory_order_relaxed);
    idempotent_.store(false, std::memory_order_relaxed);
}

bool MprpcController::Failed() const
{
    return m_failed.load(std::memory_order_acquire);
}

std::string MprpcController::ErrorText() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return m_errText;
}

void MprpcController::SetFailed(const std::string &reason)
{
    // 先写入错误文本，再发布失败标记，保证并发读取能看到错误原因。
    {
        std::lock_guard<std::mutex> lock(mu_);
        m_errText = reason;
    }
    m_failed.store(true, std::memory_order_release);
}

void MprpcController::StartCancel()
{
    // 复制回调后在锁外执行，避免回调重入 controller 时死锁。
    m_canceled.store(true, std::memory_order_release);
    std::function<void()> cb;
    {
        std::lock_guard<std::mutex> lock(mu_);
        cb = m_cancelCallback;
    }
    if (cb)
    {
        cb();
    }
}

bool MprpcController::IsCanceled() const
{
    return m_canceled.load(std::memory_order_acquire);
}

void MprpcController::setCancelCallback(std::function<void()> cb)
{
    std::lock_guard<std::mutex> lock(mu_);
    m_cancelCallback = std::move(cb);
}

void MprpcController::NotifyOnCancel(google::protobuf::Closure *callback)
{
    // Protobuf 接口保留；当前实现通过 setCancelCallback 提供取消通知。
    (void)callback;
}

void MprpcController::SetRpcTimeoutMs(int64_t ms)
{
    rpcTimeoutMs_.store(ms < 0 ? 0 : ms, std::memory_order_relaxed);
}

int64_t MprpcController::RpcTimeoutMs() const
{
    return rpcTimeoutMs_.load(std::memory_order_relaxed);
}

void MprpcController::SetDeadline(uint32_t deadlineMs)
{
    deadlineMs_.store(static_cast<int64_t>(deadlineMs), std::memory_order_relaxed);
    deadlineStartNs_.store(steadyNowNs(), std::memory_order_relaxed);
}

int64_t MprpcController::RemainingMs() const
{
    // 返回剩余预算；负值表示未设置 deadline，零表示已超时。
    const int64_t budget = deadlineMs_.load(std::memory_order_relaxed);
    if (budget == 0)
    {
        return -1; // 无 deadline
    }
    const int64_t start = deadlineStartNs_.load(std::memory_order_relaxed);
    if (start == 0)
    {
        return budget;
    }
    const int64_t elapsedNs = steadyNowNs() - start;
    const int64_t elapsedMs = elapsedNs / 1000000;
    return elapsedMs >= budget ? 0 : (budget - elapsedMs);
}
