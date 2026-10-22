#pragma once
#include <zookeeper/zookeeper.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// ZooKeeper 客户端封装：
//  * Start() 带超时，不再无限阻塞
//  * 会话断开重连后触发会话回调（临时节点需要在此重新注册）
//  * 会话过期（EXPIRED）自动重建句柄
//  * GetChildrenData() 提供带一次性 watch 的子节点列表（服务发现用）
class ZkClient
{
public:
    // isReconnect: 是否为会话（重）建立后的回调
    using SessionCallback = std::function<void(bool isReconnect)>;

    ZkClient();
    ~ZkClient();

    // 进程级共享会话（provider 注册 + channel 发现可共用一条连接）
    static ZkClient &global();

    // 连接 ZK 并等待就绪；超时返回 false，可重复调用（第二次直接返回当前状态）
    bool Start(double timeoutSec = 10.0);
    bool connected() const;

    // 注册会话回调：每次会话建立（含重连/会话重建）都会调用
    // 需要在 Start() 之前注册，否则首次连接的回调会丢失
    void addSessionCallback(void *owner, SessionCallback cb);
    void removeSessionCallbacks(void *owner);
    void removeWatch(const char *path);

    // 创建节点（幂等：已存在视为成功），失败重试若干次后返回 false，不再 exit()
    bool Create(const char *path, const char *data, int datalen, int state = 0);

    // 读取节点数据（修复了原实现未加 '\0' 的越界读）
    std::string GetData(const char *path);

    // 列出 path 的子节点数据（子节点名），同时注册一次性 children watch
    // 子节点变化时回调 onChange，需要在 onChange 里再次调用本函数以续期 watch
    bool GetChildrenData(const char *path,
                         std::vector<std::string> &children,
                         std::function<void()> onChange);

    void Close();

private:
    struct WatchCtx
    {
        // watcher 上下文由 watches_ 持有，回调使用时保持共享所有权
        ZkClient *self = nullptr;
        std::string path;
        std::function<void()> fn;
    };

    static void sessionWatcher(zhandle_t *zh, int type, int state,
                               const char *path, void *watcherCtx);
    static void childrenWatcher(zhandle_t *zh, int type, int state,
                                const char *path, void *watcherCtx);

    void handleSessionEvent(int state);
    void invokeSessionCallbacks(bool isReconnect);
    void refreshWatches();
    bool startInternal(double timeoutSec, bool announce);
    void reinitAsync();
    void reinitInWorker();
    zhandle_t *takeHandle();
    bool hasHandle() const;
    bool queueTask(std::function<void()> task);
    void workerLoop();

    std::atomic<bool> closed_{false};

    mutable std::mutex apiMu_; // 串行化 ZK API 与句柄关闭/替换
    zhandle_t *m_zhandle = nullptr;

    mutable std::mutex stateMu_;
    std::condition_variable stateCv_;
    bool connected_ = false;
    bool everConnected_ = false;

    std::mutex cbMu_;
    std::vector<std::pair<void *, SessionCallback>> sessionCallbacks_; // 按 owner 移除回调

    std::mutex watchMu_;
    std::map<std::string, std::shared_ptr<WatchCtx>> watches_; // 每条路径的当前 watch

    std::atomic<bool> reinitPending_{false}; // 合并并发触发的异步重建请求
    std::mutex taskMu_;
    std::condition_variable taskCv_;
    std::deque<std::function<void()>> tasks_;
    bool stoppingTasks_ = false;
    std::thread worker_;
};
