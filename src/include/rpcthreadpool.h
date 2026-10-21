#pragma once
// 有界业务线程池：把耗时的业务 handler 从 IO 线程剥离。
// 队列满时 submit 返回 false，由上层做准入控制（返回 429）。

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

class RpcThreadPool
{
public:
    // maxQueue 限制等待执行的任务数，不包含正在运行的任务。
    RpcThreadPool(size_t threadNum, size_t maxQueue);
    ~RpcThreadPool();

    void start(); // 幂等启动固定数量的工作线程
    void stop();  // 停止接收任务，排空队列后等待工作线程退出

    // 队列满返回 false（不阻塞调用方）
    bool submit(std::function<void()> task);

    size_t pending() const; // 当前排队任务数
    uint64_t rejected() const { return rejected_.load(std::memory_order_relaxed); }

private:
    void workerLoop();

    const size_t threadNum_;
    const size_t maxQueue_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()> > queue_;
    bool stopping_ = false; // 在 mu_ 保护下访问
    std::vector<std::thread> workers_;

    std::atomic<uint64_t> rejected_{0};
};
