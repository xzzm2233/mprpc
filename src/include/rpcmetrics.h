#pragma once
// 轻量 RPC 指标：按方法统计 QPS / 错误 / 超时 / 在途数 / 延迟分位数。
// 全进程单例，无锁读取（entry 创建后地址稳定），dump 输出到日志。

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class RpcMetrics
{
public:
    static RpcMetrics &instance();

    // scope 区分 client/server；method 为服务方法或内部事件名。
    void record(const std::string &scope, const std::string &method,
                int64_t latencyNs, bool ok, bool timeout, bool busy);

    // 维护按 scope 和方法区分的当前在途数及峰值。
    void inflightUp(const std::string &scope, const std::string &method);
    void inflightDown(const std::string &scope, const std::string &method);

    // 输出一次指标快照（日志）
    void dump();

private:
    RpcMetrics() = default;

    // 延迟直方图：64ns ~ 2^35ns(约34s)，对数分桶
    static constexpr int kBaseBucket = 6;
    static constexpr int kBucketCount = 30;

    struct Entry
    {
        std::atomic<uint64_t> calls{0};
        std::atomic<uint64_t> errors{0};
        std::atomic<uint64_t> timeouts{0};
        std::atomic<uint64_t> busy{0};
        std::atomic<uint64_t> inflight{0};
        std::atomic<uint64_t> inflightPeak{0};
        std::atomic<uint64_t> latencyTotalNs{0};
        std::atomic<uint64_t> latencyMaxNs{0};
        std::atomic<uint64_t> buckets[kBucketCount]; // 每个对数延迟区间的样本数
    };

    Entry *get(const std::string &scope, const std::string &method);
    static int bucketOf(int64_t ns);

    std::mutex mu_;
    std::map<std::string, std::unique_ptr<Entry>> entries_;
};
