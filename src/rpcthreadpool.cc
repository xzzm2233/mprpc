#include "rpcthreadpool.h"

#include <utility>

// 线程数和队列容量至少为 1，避免配置值造成无工作线程或永久拒绝
RpcThreadPool::RpcThreadPool(size_t threadNum, size_t maxQueue)
    : threadNum_(threadNum == 0 ? 1 : threadNum),
      maxQueue_(maxQueue == 0 ? 1 : maxQueue)
{
}

RpcThreadPool::~RpcThreadPool()
{
    stop();
}

void RpcThreadPool::start()
{
    // 持锁创建线程，保证并发 start 不会重复启动工作线程
    std::lock_guard<std::mutex> lock(mu_);
    if (!workers_.empty())
    {
        return;
    }
    stopping_ = false;
    workers_.reserve(threadNum_);
    for (size_t i = 0; i < threadNum_; ++i)
    {
        workers_.emplace_back([this]()
                              { workerLoop(); });
    }
}

void RpcThreadPool::stop()
{
    // 设置停止标志并唤醒所有线程；worker 会先完成已排队任务再退出
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_ && workers_.empty())
        {
            return;
        }
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto &t : workers_)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
    workers_.clear();
}

bool RpcThreadPool::submit(std::function<void()> task)
{
    // 队列满或线程池停止时立即拒绝，不阻塞提交方（通常是 IO 线程）
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_ || queue_.size() >= maxQueue_)
        {
            rejected_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        queue_.emplace_back(std::move(task));
    }
    cv_.notify_one();
    return true;
}

size_t RpcThreadPool::pending() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

void RpcThreadPool::workerLoop()
{
    // 工作线程等待任务，取出任务后在不持队列锁的情况下执行
    for (;;)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this]()
                     { return stopping_ || !queue_.empty(); });
            if (queue_.empty())
            {
                // stop 时：队列已排空，退出
                return;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        task();
    }
}
