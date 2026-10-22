#include "rpcmetrics.h"

#include <mymuduo/Logger.h>

#include <algorithm>
#include <cmath>

// 将纳秒延迟映射到对数直方图桶，超出范围时落入边界桶
int RpcMetrics::bucketOf(int64_t ns)
{
    if (ns < 1)
    {
        ns = 1;
    }
    int idx = 0;
    uint64_t v = static_cast<uint64_t>(ns);
    while (v > 1)
    {
        v >>= 1;
        ++idx; // idx == floor(log2(ns))
    }
    const int b = idx - kBaseBucket;
    return std::min(std::max(b, 0), kBucketCount - 1);
}

RpcMetrics &RpcMetrics::instance()
{
    // 函数局部静态对象确保惰性初始化且线程安全
    static RpcMetrics m;
    return m;
}

RpcMetrics::Entry *RpcMetrics::get(const std::string &scope,
                                   const std::string &method)
{
    // Entry 创建后由 unique_ptr 固定持有，返回的地址在后续插入中保持稳定
    std::string key(scope);
    key.push_back('|');
    key.append(method);

    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(key);
    if (it != entries_.end())
    {
        return it->second.get();
    }
    std::unique_ptr<Entry> e(new Entry());
    for (auto &b : e->buckets)
    {
        b.store(0, std::memory_order_relaxed);
    }
    Entry *raw = e.get();
    entries_.emplace(std::move(key), std::move(e));
    return raw;
}

void RpcMetrics::record(const std::string &scope, const std::string &method,
                        int64_t latencyNs, bool ok, bool timeout, bool busy)
{
    // 原子计数允许多个 IO/业务线程并发记录同一方法指标
    Entry *e = get(scope, method);
    e->calls.fetch_add(1, std::memory_order_relaxed);
    if (busy)
    {
        e->busy.fetch_add(1, std::memory_order_relaxed);
    }
    else if (timeout)
    {
        e->timeouts.fetch_add(1, std::memory_order_relaxed);
    }
    else if (!ok)
    {
        e->errors.fetch_add(1, std::memory_order_relaxed);
    }

    if (latencyNs > 0)
    {
        e->latencyTotalNs.fetch_add(static_cast<uint64_t>(latencyNs), std::memory_order_relaxed);
        uint64_t prev = e->latencyMaxNs.load(std::memory_order_relaxed);
        while (latencyNs > static_cast<int64_t>(prev) &&
               !e->latencyMaxNs.compare_exchange_weak(prev, static_cast<uint64_t>(latencyNs)))
        {
        }
        e->buckets[bucketOf(latencyNs)].fetch_add(1, std::memory_order_relaxed);
    }
}

void RpcMetrics::inflightUp(const std::string &scope, const std::string &method)
{
    Entry *e = get(scope, method);
    const uint64_t n = e->inflight.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t peak = e->inflightPeak.load(std::memory_order_relaxed);
    while (n > peak &&
           !e->inflightPeak.compare_exchange_weak(peak, n, std::memory_order_relaxed))
    {
    }
}

void RpcMetrics::inflightDown(const std::string &scope, const std::string &method)
{
    Entry *e = get(scope, method);
    const uint64_t n = e->inflight.load(std::memory_order_relaxed);
    if (n > 0)
    {
        e->inflight.fetch_sub(1, std::memory_order_relaxed);
    }
}

void RpcMetrics::dump()
{
    // 先在锁内复制稳定的 Entry 指针，再锁外读取并记录快照
    std::vector<std::pair<std::string, Entry *>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        snapshot.reserve(entries_.size());
        for (auto &kv : entries_)
        {
            snapshot.emplace_back(kv.first, kv.second.get());
        }
    }
    if (snapshot.empty())
    {
        return;
    }

    for (auto &kv : snapshot)
    {
        Entry *e = kv.second;
        const uint64_t calls = e->calls.load(std::memory_order_relaxed);
        const uint64_t errors = e->errors.load(std::memory_order_relaxed);
        const uint64_t timeouts = e->timeouts.load(std::memory_order_relaxed);
        const uint64_t busy = e->busy.load(std::memory_order_relaxed);
        const uint64_t inflight = e->inflight.load(std::memory_order_relaxed);
        const uint64_t peak = e->inflightPeak.load(std::memory_order_relaxed);
        const uint64_t total = e->latencyTotalNs.load(std::memory_order_relaxed);
        const uint64_t maxNs = e->latencyMaxNs.load(std::memory_order_relaxed);

        uint64_t hist = 0;
        for (int i = 0; i < kBucketCount; ++i)
        {
            hist += e->buckets[i].load(std::memory_order_relaxed);
        }

        // 用直方图分位数（上界），hist 为 0 时给 -1
        // 直方图只提供分桶上界的近似分位数
        auto quantile = [&](double q) -> int64_t
        {
            if (hist == 0)
            {
                return -1;
            }
            const uint64_t want = static_cast<uint64_t>(q * static_cast<double>(hist));
            uint64_t acc = 0;
            for (int i = 0; i < kBucketCount; ++i)
            {
                acc += e->buckets[i].load(std::memory_order_relaxed);
                if (acc > want)
                {
                    return (static_cast<int64_t>(1) << (kBaseBucket + i));
                }
            }
            return (static_cast<int64_t>(1) << (kBaseBucket + kBucketCount - 1));
        };

        const double avgUs = calls ? static_cast<double>(total) / static_cast<double>(calls) / 1000.0 : 0.0;
        LOG_INFO("rpc metrics %s calls=%lu errors=%lu timeouts=%lu busy=%lu inflight=%lu peak=%lu "
                 "avg=%.1fus p50=%ldus p99=%ldus max=%.1fus",
                 kv.first.c_str(),
                 static_cast<unsigned long>(calls),
                 static_cast<unsigned long>(errors),
                 static_cast<unsigned long>(timeouts),
                 static_cast<unsigned long>(busy),
                 static_cast<unsigned long>(inflight),
                 static_cast<unsigned long>(peak),
                 avgUs,
                 static_cast<long>(quantile(0.50) / 1000),
                 static_cast<long>(quantile(0.99) / 1000),
                 maxNs / 1000.0);
    }
}
