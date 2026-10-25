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

#include <memory>
#include <string>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

class MprpcController;

class MprpcChannel : public google::protobuf::RpcChannel, public std::enable_shared_from_this<MprpcChannel>
{
public:
    // 创建连接到服务端的 RPC 通道；调用 CallMethod 前需用 std::shared_ptr 管理本对象
    // loop 必须在通道销毁前保持有效
    MprpcChannel(EventLoop *loop, const InetAddress &serverAddr);
    ~MprpcChannel();

    // 序列化请求并异步发送；完成、失败或超时后调用 done
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done);

private:
    struct PendingCall;

    // 处理 TCP 连接变化和接收到的响应数据
    void onConnection(const TcpConnectionPtr &conn);
    void onMessage(const TcpConnectionPtr &conn, Buffer *buffer, Timestamp time);
    void onResponse(const RpcFrameHeader &header, const std::string &body);
    std::shared_ptr<PendingCall> takePendingCall(uint64_t req_id);
    void sendRequestInLoop(uint64_t req_id, const std::string &packet);

    // 将取消、超时事件切回 EventLoop 线程处理
    void onCancelCallback(uint64_t req_id);
    void cancelInLoop(uint64_t req_id);
    void onTimeout(uint64_t req_id);

    // 必须保持有效直到调用完成回调
    struct PendingCall
    {
        google::protobuf::RpcController *controller;
        google::protobuf::Message *response;
        google::protobuf::Closure *done;
        TimerId timerId;
        bool queued = false;
        bool sent = false;
    };

    EventLoop *loop_; // 非拥有指针，生命周期由创建方保证
    TcpClient client_;

    std::atomic<uint64_t> m_reqId{0}; // 当前通道内递增的请求 ID
    std::mutex m_mutex;               // 保护跨线程访问的待完成调用表
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> m_pendingCalls;
    RpcFrame responseFrame_; // 按连接保留响应半包解析进度

    // 暂存尚未发送的请求包，连接建立后按 req_id 检查 pending 状态再发送。
    std::vector<std::pair<uint64_t, std::string>> m_unsentPackets;
};