#include "zookeeperutil.h"
#include "rpcapplication.h"

#include <mymuduo/Logger.h>

#include <unistd.h>

#include <chrono>
#include <utility>

static const int kSessionTimeoutMs = 30000;
static const int kCreateRetries = 3;

// ZK 回调只转发状态变化；具体的恢复和服务发现由 ZkClient 管理。
ZkClient::ZkClient() = default;

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
    // ZooKeeper 原生会话回调入口。
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
    // 一次性 children watch 触发后转发用户回调，由用户重新注册 watch。
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

    // 投递到内部线程：watcher 运行在 ZK 的 IO 线程上，直接调同步 API 会死锁
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
    if (fn)
    {
        fn();
    }
}

void ZkClient::handleSessionEvent(int state)
{
    // 连接成功唤醒 Start() 等待者；会话过期时异步重建句柄。
    if (state == ZOO_CONNECTED_STATE)
    {
        bool isReconnect = false;
        {
            std::lock_guard<std::mutex> lock(stateMu_);
            isReconnect = connected_;
            connected_ = true;
        }
        stateCv_.notify_all();
        invokeSessionCallbacks(isReconnect);
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
    // 在锁外调用用户回调，避免回调再次注册/移除时死锁。
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
    std::lock_guard<std::mutex> lock(watchMu_);
    if (path)
        watches_.erase(path);
}

bool ZkClient::connected() const
{
    std::lock_guard<std::mutex> lock(stateMu_);
    return connected_;
}

zhandle_t *ZkClient::currentHandle() const
{
    std::lock_guard<std::mutex> lock(apiMu_);
    return m_zhandle;
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
    // 确保句柄已创建，再等待会话进入 connected 状态或客户端关闭。
    if (currentHandle() == nullptr)
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
    // 创建 ZK 句柄并等待对应的会话 watcher 通知连接结果。
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
        if (closed_)
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
                                      { return connected_ || closed_; }) &&
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
    // 合并并发重建请求，避免多个过期事件同时关闭和创建会话。
    bool expected = false;
    if (!reinitPending_.compare_exchange_strong(expected, true))
    {
        return;
    }
    // 不能在 watcher 回调里直接 zookeeper_close（会等待回调完成导致死锁），
    // 投递到内部线程处理。
    if (zhandle_t *dead = takeHandle())
    {
        zookeeper_close(dead);
    }
    if (closed_)
    {
        reinitPending_.store(false);
        return;
    }
    const bool ok = startInternal(10.0, true);
    if (!ok)
    {
        LOG_ERROR("zookeeper session rebuild failed");
    }
    reinitPending_.store(false);
}

bool ZkClient::Create(const char *path, const char *data, int datalen, int state)
{
    // 节点创建视为幂等操作；遇到并发创建则将已存在视为成功。
    for (int attempt = 0; attempt < kCreateRetries; ++attempt)
    {
        zhandle_t *zh = currentHandle();
        if (zh == nullptr)
        {
            usleep(50 * 1000);
            continue;
        }

        const int exists = zoo_exists(zh, path, 0, nullptr);
        if (exists == ZOK)
        {
            return true;
        }
        if (exists != ZNONODE)
        {
            LOG_ERROR("zoo_exists(%s) rc=%d", path, exists);
            usleep(50 * 1000 * (attempt + 1));
            continue;
        }

        char path_buffer[512] = {0};
        const int rc = zoo_create(zh, path, data, datalen, &ZOO_OPEN_ACL_UNSAFE, state,
                                  path_buffer, sizeof(path_buffer));
        if (rc == ZOK || rc == ZNODEEXISTS)
        {
            return true;
        }
        LOG_ERROR("zoo_create(%s) rc=%d", path, rc);
        usleep(50 * 1000 * (attempt + 1));
    }
    return false;
}

std::string ZkClient::GetData(const char *path)
{
    zhandle_t *zh = currentHandle();
    if (zh == nullptr)
    {
        return "";
    }
    // 数据为 "ip:port"，8KB 足够；按 zoo_get 约定 buffer_len 入参为容量、出参为实际长度
    char buffer[8 * 1024] = {0};
    int bufferlen = sizeof(buffer);
    const int rc = zoo_get(zh, path, 0, buffer, &bufferlen, nullptr);
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
    // 保存 watch 上下文并拉取子节点；ZooKeeper watch 触发后需要调用方续订。
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

    zhandle_t *zh = currentHandle();
    if (zh == nullptr)
    {
        return false;
    }

    struct String_vector vec;
    vec.count = 0;
    vec.data = nullptr;
    const int rc = zoo_wget_children(zh, path, childrenWatcher, wc.get(), &vec);
    if (rc == ZNONODE)
    {
        // 节点还不存在：挂一个 exists watch，等它出现时回调（回调里会重新拉取）
        const int erc = zoo_wexists(zh, path, childrenWatcher, wc.get(), nullptr);
        LOG_INFO("znode %s not exist yet (exists rc=%d), waiting", path, erc);
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
    // 先标记关闭并唤醒等待线程，再关闭并释放当前句柄。
    closed_ = true;
    {
        std::lock_guard<std::mutex> lock(stateMu_);
        connected_ = false;
    }
    stateCv_.notify_all();

    if (zhandle_t *zh = takeHandle())
    {
        zookeeper_close(zh);
    }
}
