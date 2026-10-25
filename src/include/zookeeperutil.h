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

// ZooKeeper 客户端封装，使用进程级会话并将耗时/同步回调操作放到后台线程
// Start() 最多等待指定时间；会话重连或重建后会恢复 watch 并通知订阅者
class ZkClient
{
public:
    // 回调在内部工作线程执行；isReconnect 表示该会话此前已成功连接
    using SessionCallback = std::function<void(bool isReconnect)>;

    ZkClient();
    ~ZkClient();

    // 获取进程级共享客户端；服务注册和服务发现可复用同一 ZooKeeper 会话
    static ZkClient &global();

    // 创建会话（如有必要）并等待连接成功；超时或关闭时返回 false
    bool Start(double timeoutSec = 10.0);
    bool connected() const;

    // 注册会话建立回调。已连接时也会异步收到当前会话通知
    void addSessionCallback(void *owner, SessionCallback cb);
    // 删除指定 owner 的全部会话回调
    void removeSessionCallbacks(void *owner);
    // 停止指定路径的 watch 通知；保留 watcher 上下文以保证晚到回调安全
    void removeWatch(const char *path);

    // 创建节点；已存在视为成功，临时/持久属性由 state 指定
    bool Create(const char *path, const char *data, int datalen, int state = 0);

    // 读取节点数据；失败时记录日志并返回空字符串
    std::string GetData(const char *path);

    // 获取子节点名称并注册一次性 watch。变化时在后台线程调用 onChange
    // 回调需再次调用本函数以续订 watch。返回 true 表示本次读取成功
    bool GetChildrenData(const char *path,
                         std::vector<std::string> &children,
                         std::function<void()> onChange);

    // 停止后台任务、关闭会话并释放 watch 上下文；关闭后不能重新启动
    void Close();

private:
    struct WatchCtx
    {
        // ZooKeeper 保存该上下文指针；直到句柄关闭前由 watches_ 保持其地址有效
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
    // 新会话建立后重新注册有效的 children/exists watch
    void refreshWatches();
    bool startInternal(double timeoutSec, bool announce);
    // 在 ZooKeeper 回调线程中只排队，不直接关闭句柄或等待重连
    void reinitAsync();
    void reinitInWorker();
    // 转移当前句柄的所有权；调用方负责关闭
    zhandle_t *takeHandle();
    bool hasHandle() const;
    bool queueTask(std::function<void()> task);
    void workerLoop();

    // 关闭标记跨 API、后台任务和 watcher 线程访问
    std::atomic<bool> closed_{false};

    // 串行化句柄读写、ZooKeeper API 调用及关闭，避免操作已释放的句柄
    mutable std::mutex apiMu_;
    zhandle_t *m_zhandle = nullptr;

    // connected_ 和 everConnected_ 仅由 stateMu_ 保护
    mutable std::mutex stateMu_;
    std::condition_variable stateCv_;
    bool connected_ = false;
    bool everConnected_ = false;

    std::mutex cbMu_;
    // owner 用于批量注销；副本在锁外执行，避免用户回调重入时死锁
    std::vector<std::pair<void *, SessionCallback>> sessionCallbacks_;

    std::mutex watchMu_;
    // 每个路径复用稳定的 WatchCtx；fn 为空表示已停用、等待会话关闭后释放
    std::map<std::string, std::shared_ptr<WatchCtx>> watches_;

    std::atomic<bool> reinitPending_{false};
    // 单线程执行 watch、会话回调和重连任务，避免阻塞 ZooKeeper 原生回调线程
    std::mutex taskMu_;
    std::condition_variable taskCv_;
    std::deque<std::function<void()>> tasks_;
    bool stoppingTasks_ = false;
    std::thread worker_;
};
