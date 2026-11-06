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
    uint64_t remainingNanoseconds = static_cast<uint64_t>(ns);
    while (remainingNanoseconds > 1)
    {
        remainingNanoseconds >>= 1;
        ++idx; // idx == floor(log2(ns))
    }
    const int bucketIndex = idx - kBaseBucket;
    return std::min(std::max(bucketIndex, 0), kBucketCount - 1);
}

RpcMetrics &RpcMetrics::instance()
{
    // 函数局部静态对象确保惰性初始化且线程安全
    static RpcMetrics metrics;
    return metrics;
}

RpcMetrics::Entry *RpcMetrics::get(const std::string &scope,
                                   const std::string &method)
{
    // Entry 创建后由 unique_ptr 固定持有，返回的地址在后续插入中保持稳定
    std::string key(scope);
    key.push_back('|');
    key.append(method);

    std::lock_guard<std::mutex> lock(mu_);
    auto metricsEntry = entries_.find(key);
    if (metricsEntry != entries_.end())
    {
        return metricsEntry->second.get();
    }
    std::unique_ptr<Entry> newEntry(new Entry());
    for (auto &bucket : newEntry->buckets)
    {
        bucket.store(0, std::memory_order_relaxed);
    }
    Entry *newEntryPointer = newEntry.get();
    entries_.emplace(std::move(key), std::move(newEntry));
    return newEntryPointer;
}

void RpcMetrics::record(const std::string &scope, const std::string &method,
                        int64_t latencyNs, bool ok, bool timeout, bool busy)
{
    // 原子计数允许多个 IO/业务线程并发记录同一方法指标
    Entry *metricsEntry = get(scope, method);
    metricsEntry->calls.fetch_add(1, std::memory_order_relaxed);
    if (busy)
    {
        metricsEntry->busy.fetch_add(1, std::memory_order_relaxed);
    }
    else if (timeout)
    {
        metricsEntry->timeouts.fetch_add(1, std::memory_order_relaxed);
    }
    else if (!ok)
    {
        metricsEntry->errors.fetch_add(1, std::memory_order_relaxed);
    }

    if (latencyNs > 0)
    {
        metricsEntry->latencyTotalNs.fetch_add(static_cast<uint64_t>(latencyNs), std::memory_order_relaxed);
        uint64_t previousMaximum = metricsEntry->latencyMaxNs.load(std::memory_order_relaxed);
        while (latencyNs > static_cast<int64_t>(previousMaximum) &&
               !metricsEntry->latencyMaxNs.compare_exchange_weak(previousMaximum,
                                                                  static_cast<uint64_t>(latencyNs)))
        {
        }
        metricsEntry->buckets[bucketOf(latencyNs)].fetch_add(1, std::memory_order_relaxed);
    }
}

void RpcMetrics::inflightUp(const std::string &scope, const std::string &method)
{
    Entry *metricsEntry = get(scope, method);
    const uint64_t inflightCount =
        metricsEntry->inflight.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t peak = metricsEntry->inflightPeak.load(std::memory_order_relaxed);
    while (inflightCount > peak &&
           !metricsEntry->inflightPeak.compare_exchange_weak(peak, inflightCount,
                                                              std::memory_order_relaxed))
    {
    }
}

void RpcMetrics::inflightDown(const std::string &scope, const std::string &method)
{
    Entry *metricsEntry = get(scope, method);
    const uint64_t inflightCount = metricsEntry->inflight.load(std::memory_order_relaxed);
    if (inflightCount > 0)
    {
        metricsEntry->inflight.fetch_sub(1, std::memory_order_relaxed);
    }
}

void RpcMetrics::dump()
{
    // 先在锁内复制稳定的 Entry 指针，再锁外读取并记录快照
    std::vector<std::pair<std::string, Entry *>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        snapshot.reserve(entries_.size());
        for (auto &metricsEntry : entries_)
        {
            snapshot.emplace_back(metricsEntry.first, metricsEntry.second.get());
        }
    }
    if (snapshot.empty())
    {
        return;
    }

    for (auto &metricEntry : snapshot)
    {
        Entry *metricsEntryPointer = metricEntry.second;
        const uint64_t calls = metricsEntryPointer->calls.load(std::memory_order_relaxed);
        const uint64_t errors = metricsEntryPointer->errors.load(std::memory_order_relaxed);
        const uint64_t timeouts = metricsEntryPointer->timeouts.load(std::memory_order_relaxed);
        const uint64_t busy = metricsEntryPointer->busy.load(std::memory_order_relaxed);
        const uint64_t inflight = metricsEntryPointer->inflight.load(std::memory_order_relaxed);
        const uint64_t peak = metricsEntryPointer->inflightPeak.load(std::memory_order_relaxed);
        const uint64_t total = metricsEntryPointer->latencyTotalNs.load(std::memory_order_relaxed);
        const uint64_t maxNs = metricsEntryPointer->latencyMaxNs.load(std::memory_order_relaxed);

        uint64_t histogramCount = 0;
        for (int bucketIndex = 0; bucketIndex < kBucketCount; ++bucketIndex)
        {
            histogramCount += metricsEntryPointer->buckets[bucketIndex].load(std::memory_order_relaxed);
        }

        // 用直方图分位数（上界），hist 为 0 时给 -1
        // 直方图只提供分桶上界的近似分位数
        auto quantile = [&](double quantileFraction) -> int64_t
        {
            if (histogramCount == 0)
            {
                return -1;
            }
            const uint64_t targetCount =
                static_cast<uint64_t>(quantileFraction * static_cast<double>(histogramCount));
            uint64_t cumulativeCount = 0;
            for (int bucketIndex = 0; bucketIndex < kBucketCount; ++bucketIndex)
            {
                cumulativeCount +=
                    metricsEntryPointer->buckets[bucketIndex].load(std::memory_order_relaxed);
                if (cumulativeCount > targetCount)
                {
                    return (static_cast<int64_t>(1) << (kBaseBucket + bucketIndex));
                }
            }
            return (static_cast<int64_t>(1) << (kBaseBucket + kBucketCount - 1));
        };

        const double avgUs = calls ? static_cast<double>(total) / static_cast<double>(calls) / 1000.0 : 0.0;
        LOG_INFO("rpc metrics %s calls=%lu errors=%lu timeouts=%lu busy=%lu inflight=%lu peak=%lu "
                 "avg=%.1fus p50=%ldus p99=%ldus max=%.1fus",
                 metricEntry.first.c_str(),
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
