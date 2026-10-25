#include "zookeeperutil.h"
#include "rpcapplication.h"

#include <mymuduo/Logger.h>

#include <unistd.h>

#include <chrono>
#include <exception>
#include <utility>

static const int kSessionTimeoutMs = 30000;
static const int kCreateRetries = 3;

// 启动客户端专属后台线程；ZooKeeper 原生 watcher 只负责派发轻量事件，
// watcher 后续处理、用户回调和会话重建由该线程串行执行。
ZkClient::ZkClient()
    : worker_(&ZkClient::workerLoop, this)
{
}

ZkClient::~ZkClient()
{
    Close();
}

ZkClient &ZkClient::global()
{
    static ZkClient client;
    return client;
}

void ZkClient::sessionWatcher(zhandle_t *, int type, int state, const char *, void *watcherCtx)
{
    // ZooKeeper 原生会话回调入口，不在此线程执行阻塞操作
    if (type != ZOO_SESSION_EVENT)
    {
        return;
    }
    ZkClient *self = static_cast<ZkClient *>(watcherCtx);
    if (self)
    {
        self->handleSessionEvent(state);
    }
}

void ZkClient::childrenWatcher(zhandle_t *, int type, int state, const char *path, void *watcherCtx)
{
    // Watch 是一次性的；触发后转发订阅回调，由订阅方重新读取以续订
    (void)state;
    (void)path;
    if (type == ZOO_SESSION_EVENT)
    {
        // 会话事件由 sessionWatcher 处理（重连后统一重新拉取并续期 watch）
        return;
    }
    WatchCtx *wc = static_cast<WatchCtx *>(watcherCtx);
    if (!wc || !wc->self)
    {
        return;
    }
    ZkClient *self = wc->self;

    // 先复制回调并释放锁，再排队，避免在 ZooKeeper 原生 IO 线程调用同步 API
    std::function<void()> fn;
    {
        std::lock_guard<std::mutex> lock(self->watchMu_);
        auto it = self->watches_.find(wc->path);
        if (it == self->watches_.end() || !it->second->fn)
        {
            return;
        }
        fn = it->second->fn;
    }
    self->queueTask(std::move(fn));
}

void ZkClient::handleSessionEvent(int state)
{
    // 连接成功唤醒 Start() 等待者；重连恢复与用户通知在后台线程排队执行
    if (state == ZOO_CONNECTED_STATE)
    {
        bool isReconnect = false;
        {
            std::lock_guard<std::mutex> lock(stateMu_);
            isReconnect = everConnected_;
            connected_ = true;
            everConnected_ = true;
        }
        stateCv_.notify_all();
        queueTask([this, isReconnect]()
                  {
                      refreshWatches();
                      invokeSessionCallbacks(isReconnect);
                  });
        return;
    }

    if (state == ZOO_EXPIRED_SESSION_STATE || state == ZOO_AUTH_FAILED_STATE)
    {
        LOG_ERROR("zookeeper session expired/auth failed, will rebuild session");
        {
            std::lock_guard<std::mutex> lock(stateMu_);
            connected_ = false;
        }
        stateCv_.notify_all();
        reinitAsync();
        return;
    }

    // DISCONNECTED / CONNECTING / ASSOCIATING...
    std::lock_guard<std::mutex> lock(stateMu_);
    connected_ = false;
    stateCv_.notify_all();
}

void ZkClient::invokeSessionCallbacks(bool isReconnect)
{
    // 锁内快照、锁外调用，允许回调安全地注册或移除其他订阅
    std::vector<std::pair<void *, SessionCallback>> copy;
    {
        std::lock_guard<std::mutex> lock(cbMu_);
        copy = sessionCallbacks_;
    }
    for (auto &p : copy)
    {
        if (p.second)
            p.second(isReconnect);
    }
}

void ZkClient::addSessionCallback(void *owner, SessionCallback cb)
{
    std::lock_guard<std::mutex> lock(cbMu_);
    sessionCallbacks_.push_back(std::make_pair(owner, std::move(cb)));
}

void ZkClient::removeSessionCallbacks(void *owner)
{
    std::lock_guard<std::mutex> lock(cbMu_);
    for (auto it = sessionCallbacks_.begin(); it != sessionCallbacks_.end();)
    {
        if (it->first == owner)
            it = sessionCallbacks_.erase(it);
        else
            ++it;
    }
}

void ZkClient::removeWatch(const char *path)
{
    // 不销毁 WatchCtx：ZooKeeper 可能仍持有传入的裸指针，直到会话关闭
    std::lock_guard<std::mutex> lock(watchMu_);
    if (path)
    {
        auto it = watches_.find(path);
        if (it != watches_.end())
        {
            it->second->fn = nullptr;
        }
    }
}

bool ZkClient::connected() const
{
    std::lock_guard<std::mutex> lock(stateMu_);
    return connected_;
}

bool ZkClient::hasHandle() const
{
    std::lock_guard<std::mutex> lock(apiMu_);
    return m_zhandle != nullptr;
}

zhandle_t *ZkClient::takeHandle()
{
    std::lock_guard<std::mutex> lock(apiMu_);
    zhandle_t *zh = m_zhandle;
    m_zhandle = nullptr;
    return zh;
}

bool ZkClient::Start(double timeoutSec)
{
    // 句柄创建与连接等待分开；并发 Start 调用可复用同一会话
    if (!hasHandle())
    {
        if (!startInternal(timeoutSec, true))
        {
            return false;
        }
    }

    std::unique_lock<std::mutex> lock(stateMu_);
    return stateCv_.wait_for(lock, std::chrono::duration<double>(timeoutSec),
                             [this]()
                             { return connected_ || closed_; }) &&
           connected_;
}

bool ZkClient::startInternal(double timeoutSec, bool announce)
{
    // apiMu_ 保护句柄创建；stateCv_ 等待 sessionWatcher 报告连接或关闭
    std::string host = MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    if (host.empty() || port.empty())
    {
        LOG_ERROR("zookeeperip/zookeeperport not configured");
        return false;
    }
    const std::string constr = host + ":" + port;

    {
        std::lock_guard<std::mutex> lock(apiMu_);
        if (closed_.load(std::memory_order_acquire))
        {
            return false;
        }
        if (m_zhandle == nullptr)
        {
            m_zhandle = zookeeper_init(constr.c_str(), sessionWatcher, kSessionTimeoutMs,
                                       nullptr, this, 0);
            if (m_zhandle == nullptr)
            {
                LOG_ERROR("zookeeper_init failed: %s", constr.c_str());
                return false;
            }
        }
    }

    std::unique_lock<std::mutex> lock(stateMu_);
    const bool ok = stateCv_.wait_for(lock, std::chrono::duration<double>(timeoutSec),
                                      [this]()
                                      { return connected_ || closed_.load(std::memory_order_acquire); }) &&
                    connected_;
    if (ok)
    {
        if (announce)
        {
            LOG_INFO("zookeeper connected: %s", constr.c_str());
        }
    }
    else
    {
        LOG_ERROR("zookeeper connect timeout after %.1fs: %s", timeoutSec, constr.c_str());
    }
    return ok;
}

void ZkClient::reinitAsync()
{
    // 合并重复的过期通知，并将 close/init/等待操作移出 ZooKeeper 回调线程
    bool expected = false;
    if (!reinitPending_.compare_exchange_strong(expected, true))
    {
        return;
    }
    if (!queueTask([this]()
                   { reinitInWorker(); }))
    {
        reinitPending_.store(false, std::memory_order_release);
    }
}

void ZkClient::reinitInWorker()
{
    if (zhandle_t *dead = takeHandle())
    {
        zookeeper_close(dead);
    }
    if (closed_.load(std::memory_order_acquire))
    {
        reinitPending_.store(false, std::memory_order_release);
        return;
    }
    const bool ok = startInternal(10.0, true);
    if (!ok)
    {
        LOG_ERROR("zookeeper session rebuild failed");
    }
    reinitPending_.store(false, std::memory_order_release);
}

bool ZkClient::Create(const char *path, const char *data, int datalen, int state)
{
    if (!path || datalen < 0 || (datalen > 0 && !data))
    {
        LOG_ERROR("invalid ZooKeeper create arguments");
        return false;
    }

    // exists 与 create 在同一把句柄锁下执行；并发创建由 ZNODEEXISTS 视为成功
    for (int attempt = 0; attempt < kCreateRetries; ++attempt)
    {
        int exists = ZINVALIDSTATE;
        int rc = ZINVALIDSTATE;
        {
            std::lock_guard<std::mutex> lock(apiMu_);
            if (m_zhandle)
            {
                exists = zoo_exists(m_zhandle, path, 0, nullptr);
                if (exists == ZNONODE)
                {
                    char path_buffer[512] = {0};
                    rc = zoo_create(m_zhandle, path, data, datalen, &ZOO_OPEN_ACL_UNSAFE,
                                    state, path_buffer, sizeof(path_buffer));
                }
            }
        }
        if (exists == ZOK)
        {
            return true;
        }
        if (exists == ZNONODE && (rc == ZOK || rc == ZNODEEXISTS))
        {
            return true;
        }
        if (exists != ZNONODE)
        {
            LOG_ERROR("zoo_exists(%s) rc=%d", path, exists);
        }
        else
        {
            LOG_ERROR("zoo_create(%s) rc=%d", path, rc);
        }
        usleep(50 * 1000 * (attempt + 1));
    }
    return false;
}

std::string ZkClient::GetData(const char *path)
{
    if (!path)
    {
        LOG_ERROR("invalid ZooKeeper get-data path");
        return "";
    }
    std::lock_guard<std::mutex> lock(apiMu_);
    if (m_zhandle == nullptr)
    {
        return "";
    }
    // 数据为 "ip:port"，8KB 足够；按 zoo_get 约定 buffer_len 入参为容量、出参为实际长度
    char buffer[8 * 1024] = {0};
    int bufferlen = sizeof(buffer);
    const int rc = zoo_get(m_zhandle, path, 0, buffer, &bufferlen, nullptr);
    if (rc != ZOK)
    {
        LOG_ERROR("zoo_get(%s) rc=%d", path, rc);
        return "";
    }
    if (bufferlen < 0)
    {
        return "";
    }
    if (bufferlen >= static_cast<int>(sizeof(buffer)))
    {
        LOG_ERROR("zoo_get(%s) data truncated: %d bytes", path, bufferlen);
    }
    return std::string(buffer, static_cast<size_t>(bufferlen));
}

bool ZkClient::GetChildrenData(const char *path,
                               std::vector<std::string> &children,
                               std::function<void()> onChange)
{
    if (!path)
    {
        LOG_ERROR("invalid ZooKeeper children path");
        return false;
    }

    // WatchCtx 地址在句柄关闭前保持稳定；同一路径更新回调但复用上下文
    children.clear();

    std::shared_ptr<WatchCtx> wc;
    {
        std::lock_guard<std::mutex> lock(watchMu_);
        auto &slot = watches_[path];
        if (!slot)
        {
            slot = std::make_shared<WatchCtx>();
            slot->self = this;
            slot->path = path;
        }
        wc = slot;
        wc->fn = std::move(onChange);
    }

    struct String_vector vec;
    vec.count = 0;
    vec.data = nullptr;
    int rc = ZINVALIDSTATE;
    int existsRc = ZINVALIDSTATE;
    {
        std::lock_guard<std::mutex> lock(apiMu_);
        if (m_zhandle)
        {
            rc = zoo_wget_children(m_zhandle, path, childrenWatcher, wc.get(), &vec);
            if (rc == ZNONODE)
            {
                existsRc = zoo_wexists(m_zhandle, path, childrenWatcher, wc.get(), nullptr);
            }
        }
    }
    if (rc == ZNONODE)
    {
        // 节点还不存在：挂一个 exists watch，等它出现时回调（回调里会重新拉取）
        LOG_INFO("znode %s not exist yet (exists rc=%d), waiting", path, existsRc);
        return false;
    }
    if (rc != ZOK)
    {
        LOG_ERROR("zoo_wget_children(%s) rc=%d", path, rc);
        return false;
    }

    children.reserve(static_cast<size_t>(vec.count));
    for (int i = 0; i < vec.count; ++i)
    {
        children.emplace_back(vec.data[i]);
    }
    deallocate_String_vector(&vec);
    return true;
}

void ZkClient::Close()
{
    // 先阻止新任务并 join 工作线程，再关闭句柄；这样在途 watcher 不会访问已释放上下文
    const bool alreadyClosed = closed_.exchange(true, std::memory_order_acq_rel);
    {
        std::lock_guard<std::mutex> lock(stateMu_);
        connected_ = false;
    }
    stateCv_.notify_all();

    if (!alreadyClosed)
    {
        {
            std::lock_guard<std::mutex> lock(taskMu_);
            stoppingTasks_ = true;
        }
        taskCv_.notify_all();
        if (worker_.joinable())
        {
            worker_.join();
        }
    }

    if (zhandle_t *zh = takeHandle())
    {
        zookeeper_close(zh);
    }

    std::lock_guard<std::mutex> lock(watchMu_);
    watches_.clear();
}

bool ZkClient::queueTask(std::function<void()> task)
{
    // 与 Close 共用 taskMu_，保证关闭后不会再有新任务入队
    std::lock_guard<std::mutex> lock(taskMu_);
    if (stoppingTasks_ || closed_.load(std::memory_order_acquire))
    {
        return false;
    }
    tasks_.push_back(std::move(task));
    taskCv_.notify_one();
    return true;
}

void ZkClient::workerLoop()
{
    // 用户回调异常不能让唯一的后台工作线程意外退出
    for (;;)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(taskMu_);
            taskCv_.wait(lock, [this]()
                         { return stoppingTasks_ || !tasks_.empty(); });
            if (stoppingTasks_)
            {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop_front();
        }
        try
        {
            task();
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("ZooKeeper background task failed: %s", e.what());
        }
        catch (...)
        {
            LOG_ERROR("ZooKeeper background task failed with unknown exception");
        }
    }
}

void ZkClient::refreshWatches()
{
    // 先快照有效订阅，避免持有 watchMu_ 调用 ZooKeeper API 或用户代码
    std::vector<std::pair<std::string, std::function<void()>>> watches;
    {
        std::lock_guard<std::mutex> lock(watchMu_);
        for (const auto &entry : watches_)
        {
            if (entry.second->fn)
            {
                watches.emplace_back(entry.first, entry.second->fn);
            }
        }
    }

    for (const auto &watch : watches)
    {
        std::vector<std::string> children;
        GetChildrenData(watch.first.c_str(), children, watch.second);
    }
}
