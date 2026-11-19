#pragma once
#include "rpcframe.h"

#include <mymuduo/TcpClient.h>
#include <mymuduo/EventLoop.h>
#include <mymuduo/InetAddress.h>
#include <mymuduo/TcpConnection.h>
#include <mymuduo/Buffer.h>
#include <mymuduo/Timestamp.h>
#include <mymuduo/TimerId.h>

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstddef>
#include <condition_variable>
#include <functional>
#include <memory>
#include <string>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <unordered_set>
#include <vector>

class MprpcController;

class MprpcChannel : public google::protobuf::RpcChannel, public std::enable_shared_from_this<MprpcChannel>
{
public:
    // 创建连接到服务端的 RPC 通道，调用 CallMethod 前需用 std::shared_ptr 管理本对象
    // loop 必须在通道销毁前保持有效
    MprpcChannel(EventLoop *loop, const InetAddress &serverAddr);
    // 取消定时器并以失败状态完成所有未结束的调用
    ~MprpcChannel();

    // 序列化并发送 RPC，成功、失败或超时后调用 done
    // 参数校验失败时 done 可能在本函数返回前执行，controller、response 和 done 需保持有效至回调结束
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done);

private:
    friend class MprpcDiscoveryChannel;

    struct CallbackOwner
    {
        // TCP 回调只持弱引用，锁定成功后再调用，避免通道销毁后的悬空 this
        std::mutex mutex;
        std::weak_ptr<MprpcChannel> channel;
    };

    struct PendingCall;

    // 处理 TCP 连接变化和接收到的响应数据
    void onConnection(const TcpConnectionPtr &conn);
    void onMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time);
    void onResponse(const RpcFrameHeader &header, const std::string &body);
    // 从待完成表移除请求，并清理尚未发送的请求包
    std::shared_ptr<PendingCall> takePendingCall(uint64_t req_id);
    // 在 EventLoop 中发送请求，连接未就绪时将请求包暂存一次
    void sendRequestInLoop(uint64_t req_id, const std::string &packet);

    // 将取消、超时事件切回 EventLoop 线程处理
    void onCancelCallback(uint64_t req_id);
    void cancelInLoop(uint64_t req_id);
    void onTimeout(uint64_t req_id);
    // 仅请求已发送且连接仍有效时发送协作式取消帧
    void sendCancelInLoop(uint64_t req_id,
                          const std::shared_ptr<PendingCall> &call);
    // 返回待完成表中的调用数，供发现通道判断连接是否可回收
    size_t pendingCallCount() const;

    // 必须保持有效直到调用完成回调
    struct PendingCall
    {
        google::protobuf::RpcController *controller;
        google::protobuf::Message *response;
        google::protobuf::Closure *done;
        TimerId timerId;
        std::weak_ptr<TcpConnection> connection;
        bool queued = false;
        bool sent = false;
    };

    EventLoop *loop_; // 非拥有指针，生命周期由创建方保证
    TcpClient client_;
    std::shared_ptr<CallbackOwner> callbackOwner_;
    std::mutex disconnectedCallbackMutex_;

    std::atomic<uint64_t> m_reqId{0}; // 当前通道内递增的请求 ID
    mutable std::mutex m_mutex;       // 保护跨线程访问的待完成调用表
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> m_pendingCalls;
    RpcFrame responseFrame_; // 按连接保留响应半包解析进度

    // 暂存尚未发送的请求包，连接建立后按 req_id 检查 pending 状态再发送。
    std::vector<std::pair<uint64_t, std::string>> m_unsentPackets;
    // 发现实例摘除时，在连接真正断开后通知外层回收子通道
    std::function<void()> disconnectedCallback_;
};

// 通过 ZooKeeper 的 /service/method 子节点发现实例，并按请求轮询实例
// 构造时最多等待 ZooKeeper 连接超时，调用期间应由 std::shared_ptr 保持对象有效
class MprpcDiscoveryChannel : public google::protobuf::RpcChannel,
                              public std::enable_shared_from_this<MprpcDiscoveryChannel>
{
public:
    MprpcDiscoveryChannel(EventLoop *loop, const std::string &serviceName,
                          double zkConnectTimeoutSec = 10.0);
    ~MprpcDiscoveryChannel();

    // 按方法轮询 ZooKeeper 发现的实例，无可用实例时立即失败
    // 参数校验失败时 done 可能同步执行，controller、response 和 done 需保持有效至回调结束
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;

private:
    friend class DiscoveryDone;

    // 读取方法实例并续订对应路径的 watch
    void RefreshMethod(const std::string &methodKey, const std::string &path);
    // 校验、排序并应用实例列表，然后安排回收不再使用的连接
    void ApplyEndpointUpdate(const std::string &methodKey,
                             const std::vector<std::string> &children);
    // 将连接回收任务切回 EventLoop 执行
    void SchedulePrune();
    // 断开无可用实例引用且没有待完成调用的客户端
    void PruneRetiredClients();
    // 校验 IPv4:port 格式并转换为 InetAddress
    bool ParseEndpoint(const std::string &endpoint, InetAddress *address) const;

    EventLoop *loop_;
    const std::string serviceName_;
    const double zkConnectTimeoutSec_;
    mutable std::mutex discoveryMutex_;
    std::condition_variable watchInitCv_;
    std::unordered_map<std::string, std::vector<std::string>> methodEndpoints_;
    std::unordered_map<std::string, size_t> nextEndpoint_;
    std::unordered_map<std::string, std::shared_ptr<MprpcChannel>> clients_;
    std::unordered_set<std::string> watchedPaths_;
    std::unordered_set<std::string> initializingWatchPaths_;
    bool zkReady_ = false;
};
