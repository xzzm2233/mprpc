#include "rpcprovider.h"

#include "rpcapplication.h"
#include "rpccontroller.h"
#include "rpcmetrics.h"
#include "zookeeperutil.h"

#include <mymuduo/Logger.h>

#include <zstd.h>

#include <chrono>
#include <cerrno>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <cstdio>
#include <thread>
#include <utility>

// 每个 IO loop 线程独立维护连接解析状态；同一连接的事件固定由同一 loop 处理
// 每个 IO 线程自己的连接解析状态
// muduo 保证一条连接的读写事件固定落在同一个 loop 线程，因此无需加锁
struct RpcProviderConnState
{
    RpcFrame frame;
    std::shared_ptr<RpcConnStats> stats;
};

static std::unordered_map<const TcpConnection *, RpcProviderConnState> &
RpcProviderConnStates()
{
    static thread_local std::unordered_map<const TcpConnection *, RpcProviderConnState> states;
    return states;
}

static int64_t RpcProviderSteadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 注册到 ZK 的一个节点
struct RpcProviderZkNode
{
    std::string path;
    std::string data;
    bool ephemeral;
};

static int LoadBoundedInt(MprpcConfig &config, const std::string &key,
                          int defaultValue, int minimum, int maximum)
{
    const int value = config.LoadInt(key, defaultValue);
    if (value < minimum || value > maximum)
    {
        LOG_ERROR("config [%s]=%d outside valid range [%d,%d], using %d",
                  key.c_str(), value, minimum, maximum, defaultValue);
        return defaultValue;
    }
    return value;
}

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    // 注册服务描述符和方法表；Run() 启动后这些表只读
    if (!service)
    {
        LOG_ERROR("cannot register a null protobuf service");
        return;
    }
    const google::protobuf::ServiceDescriptor *serviceDesc = service->GetDescriptor();

    ServiceInfo info;
    info.name = serviceDesc->name();
    info.service = service;

    LOG_INFO("service_name:%s:", info.name.c_str());
    for (int i = 0; i < serviceDesc->method_count(); ++i)
    {
        const google::protobuf::MethodDescriptor *methodDesc = serviceDesc->method(i);
        info.methods.emplace(methodDesc->name(), methodDesc);
        LOG_INFO("method_name:%s:", methodDesc->name().c_str());
    }

    if (!m_serviceMap.emplace(info.name, std::move(info)).second)
    {
        LOG_ERROR("duplicate protobuf service registration: %s", serviceDesc->name().c_str());
    }
}

void RpcProvider::Run()
{
    // 先加载资源限制和线程配置，再启动依赖服务的网络监听
    auto &cfg = MprpcApplication::GetInstance().GetConfig();

    const std::string ip = cfg.Load("rpcserverip");
    const std::string portStr = cfg.Load("rpcserverport");
    if (ip.empty() || portStr.empty())
    {
        LOG_ERROR("rpcserverip/rpcserverport not configured");
        exit(EXIT_FAILURE);
    }
    errno = 0;
    char *portEnd = nullptr;
    const long parsedPort = std::strtol(portStr.c_str(), &portEnd, 10);
    if (errno == ERANGE || portEnd == portStr.c_str() || *portEnd != '\0' ||
        parsedPort < 1 || parsedPort > 65535)
    {
        LOG_ERROR("invalid rpcserverport: %s", portStr.c_str());
        exit(EXIT_FAILURE);
    }
    const uint16_t port = static_cast<uint16_t>(parsedPort);

    m_ioThreads = LoadBoundedInt(cfg, "rpcserveriothreads", 4, 1, 256);
    m_maxInflight = LoadBoundedInt(cfg, "rpcmaxinflight", 1024, 1, INT_MAX);
    m_highWater = static_cast<size_t>(LoadBoundedInt(cfg, "rpchighwater", 8 * 1024 * 1024, 1, INT_MAX));
    m_compress = cfg.LoadBool("rpccompress", true);
    m_compressLevel = LoadBoundedInt(cfg, "rpccompresslevel", 3, ZSTD_minCLevel(), ZSTD_maxCLevel());
    m_compressMin = static_cast<size_t>(LoadBoundedInt(cfg, "rpccompressmin", 8 * 1024, 0, INT_MAX));
    m_metricsInterval = LoadBoundedInt(cfg, "rpcmetricsinterval", 10, 0, INT_MAX);

    const unsigned hw = std::thread::hardware_concurrency();
    const int defaultWorkers = hw && hw <= static_cast<unsigned>(INT_MAX / 2)
                                   ? static_cast<int>(hw * 2)
                                   : 8;
    const int workers = LoadBoundedInt(cfg, "rpcworkers", defaultWorkers, 1, 4096);
    const int workerQueue = LoadBoundedInt(cfg, "rpcworkerqueue", 4096, 1, INT_MAX);

    m_workers.reset(new RpcThreadPool(static_cast<size_t>(workers),
                                      static_cast<size_t>(workerQueue)));
    m_workers->start();

    InetAddress address(port, ip);
    TcpServer server(&m_eventLoop, address, "RpcProvider");

    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this,
                                           std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::OnMessage, this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3));
    server.setThreadNum(m_ioThreads);

    // ---- ZooKeeper 注册（可关闭；关闭后仅直连地址可用）----
    const bool useZk = cfg.LoadBool("zooenabled", !cfg.Load("zookeeperip", "").empty());
    if (useZk)
    {
        // 注册内容在启动时一次性算好并按值捕获：会话回调由 ZK 工作线程执行
        // 不再依赖 this，因此 provider 销毁后回调也不会踩野指针
        auto nodes = std::make_shared<std::vector<RpcProviderZkNode>>();
        const std::string ep = ip + ":" + portStr;
        for (auto &sp : m_serviceMap)
        {
            nodes->push_back({"/" + sp.first, "", false});
            for (auto &mp : sp.second.methods)
            {
                nodes->push_back({"/" + sp.first + "/" + mp.first, "", false});
                // 每个实例注册独立的临时子节点：多实例可同时注册、下线自动消失
                nodes->push_back({"/" + sp.first + "/" + mp.first + "/" + ep, ep, true});
            }
        }

        ZkClient &zk = ZkClient::global();
        zk.addSessionCallback(this, [nodes](bool isReconnect)
                              {
            if (isReconnect)
            {
                LOG_INFO("zookeeper session re-established, re-registering ephemeral nodes");
            }
            for (const auto &n : *nodes)
            {
                ZkClient::global().Create(n.path.c_str(),
                                          n.data.empty() ? nullptr : n.data.c_str(),
                                          static_cast<int>(n.data.size()),
                                          n.ephemeral ? ZOO_EPHEMERAL : 0);
            } });

        double zkTimeout = cfg.LoadDouble("rpczkconnecttimeout", 10.0);
        if (!std::isfinite(zkTimeout) || zkTimeout <= 0.0)
        {
            LOG_ERROR("invalid rpczkconnecttimeout=%.3f, using 10 seconds", zkTimeout);
            zkTimeout = 10.0;
        }
        if (!zk.Start(zkTimeout))
        {
            // 句柄仍在后台重试：ZK 恢复后会话建立会自动补注册
            LOG_ERROR("zookeeper unreachable after %.1fs, provider starts WITHOUT registry "
                      "(discovery clients cannot find it until zookeeper recovers)",
                      zkTimeout);
        }
    }
    else
    {
        LOG_INFO("zooenabled=0 (or zookeeperip empty): provider runs without service registry");
    }

    if (m_metricsInterval > 0)
    {
        m_eventLoop.runEvery(m_metricsInterval, [this]()
                             { DumpMetrics(); });
    }

    LOG_INFO("RpcProvider start service at ip:%s port:%u iothreads=%d workers=%d "
             "maxinflight=%d compress=%d",
             ip.c_str(), port, m_ioThreads, workers, m_maxInflight, m_compress ? 1 : 0);

    server.start();
    m_eventLoop.loop();

    // ---- loop 退出，回收资源 ----
    if (useZk)
    {
        ZkClient::global().removeSessionCallbacks(this);
    }
    if (m_workers)
    {
        m_workers->stop();
    }
}

void RpcProvider::OnConnection(const TcpConnectionPtr &conn)
{
    // 新连接初始化解析状态；关闭时清除线程本地状态
    if (conn->connected())
    {
        RpcProviderConnState &st = RpcProviderConnStates()[conn.get()];
        if (!st.stats)
        {
            st.stats = std::make_shared<RpcConnStats>();
        }
        st.frame.reset();

        // 背压：对端不消费时限制积压，避免 outputBuffer 无限增长
        conn->setHighWaterMarkCallback(
            [this](const TcpConnectionPtr &c, size_t mark)
            { OnHighWater(c, mark); },
            m_highWater);
        conn->setWriteCompleteCallback(
            [this](const TcpConnectionPtr &c)
            { OnWriteComplete(c); });

        LOG_INFO("new connection: %s", conn->peerAddress().toIpPort().c_str());
    }
    else
    {
        RpcProviderConnStates().erase(conn.get());
        LOG_INFO("connection closed: %s", conn->peerAddress().toIpPort().c_str());
    }
}

void RpcProvider::OnHighWater(const TcpConnectionPtr &conn, size_t mark)
{
    // 输出缓存持续增长时对该连接限流，反复触发则关闭连接
    auto it = RpcProviderConnStates().find(conn.get());
    if (it == RpcProviderConnStates().end() || !it->second.stats)
    {
        return;
    }
    RpcConnStats &st = *it->second.stats;
    ++st.highWaterHits;

    if (st.highWaterHits >= 2)
    {
        // 缓冲已越过水位又再次越过：对端完全不消费，断开保护自己
        LOG_ERROR("peer not draining output buffer (mark=%zu), force close %s",
                  mark, conn->peerAddress().toIpPort().c_str());
        RpcMetrics::instance().record("server", "__backpressure__", 0, false, false, true);
        conn->shutdown();
        return;
    }

    st.overloaded.store(true, std::memory_order_relaxed);
    LOG_INFO("output buffer hit high water mark %zu, shedding new requests on %s",
             mark, conn->peerAddress().toIpPort().c_str());
}

void RpcProvider::OnWriteComplete(const TcpConnectionPtr &conn)
{
    auto it = RpcProviderConnStates().find(conn.get());
    if (it == RpcProviderConnStates().end() || !it->second.stats)
    {
        return;
    }
    it->second.stats->highWaterHits = 0;
    it->second.stats->overloaded.store(false, std::memory_order_relaxed);
}

void RpcProvider::OnMessage(const TcpConnectionPtr &conn,
                            Buffer *buffer,
                            Timestamp time)
{
    // 一次读事件中尽可能消费所有完整帧，尾部不完整数据留待下次继续解析
    (void)time;

    RpcProviderConnState &st = RpcProviderConnStates()[conn.get()];
    if (!st.stats)
        st.stats = std::make_shared<RpcConnStats>();

    for (;;)
    {
        const char *base = buffer->peek();
        const size_t readable = buffer->readableBytes();

        // 帧头只解析一次：进度保存在 st.frame 里，半包不会反复 parse
        const RpcFrameFeedResult r = FeedRpcFrame(base, readable, &st.frame);
        if (r == RpcFrameFeedResult::kNeedMore)
        {
            return;
        }
        if (r == RpcFrameFeedResult::kError)
        {
            LOG_ERROR("malformed frame from %s (headerSize=%u), closing",
                      conn->peerAddress().toIpPort().c_str(), st.frame.headerSize);
            RpcMetrics::instance().record("server", "__badframe__", 0, false, false, false);
            buffer->retrieveAll();
            st.frame.reset();
            conn->shutdown(); // 流已无法定界，只能断开
            return;
        }

        const size_t total = st.frame.totalSize();
        // body 需要拷贝：交给工作线程后，inputBuffer 会被后续读写改写/搬移
        std::string args(base + 4 + st.frame.headerSize,
                         st.frame.header.args_size());
        const int64_t startNs = st.frame.startNs;
        buffer->retrieve(total);

        ProcessRequest(conn, st.stats, st.frame, std::move(args), startNs);
        st.frame.reset();
    }
}

void RpcProvider::ProcessRequest(const TcpConnectionPtr &conn,
                                 const std::shared_ptr<RpcConnStats> &stats,
                                 const RpcFrame &frame,
                                 const std::string &args,
                                 int64_t startNs)
{
    // 校验准入、deadline、服务方法及 protobuf body 后再投递业务线程池
    const RpcFrameHeader &header = frame.header;
    const uint64_t reqId = header.req_id();
    const uint64_t traceId = header.trace_id();
    const std::string methodKey = header.service_name() + "." + header.method_name();
    const int64_t now = RpcProviderSteadyNowNs();

    // ---- 准入控制：连接积压 / 在途超限 → 直接拒掉，不进业务线程池 ----
    if (stats->overloaded.load(std::memory_order_relaxed) ||
        stats->inflight.load(std::memory_order_relaxed) >= m_maxInflight)
    {
        RpcMetrics::instance().record("server", methodKey, now - startNs, false, false, true);
        SendError(conn, reqId, traceId, kRpcFrameServerBusy,
                  "server busy: inflight limit or output backpressure");
        return;
    }

    // ---- deadline 已耗尽：不再消耗业务资源 ----
    int64_t deadlineNs = 0;
    if (header.deadline_ms() > 0)
    {
        deadlineNs = frame.startNs +
                     static_cast<int64_t>(header.deadline_ms()) * 1000000;
        if (now >= deadlineNs)
        {
            RpcMetrics::instance().record("server", methodKey, now - startNs, false, true, false);
            SendError(conn, reqId, traceId, kRpcFrameDeadlineExceeded,
                      "deadline exceeded before dispatch");
            return;
        }
    }

    // ---- 服务 / 方法查找 ----
    auto sit = m_serviceMap.find(header.service_name());
    if (sit == m_serviceMap.end())
    {
        RpcMetrics::instance().record("server", methodKey, now - startNs, false, false, false);
        SendError(conn, reqId, traceId, kRpcFrameNotFound,
                  header.service_name() + " not exist");
        return;
    }
    auto mit = sit->second.methods.find(header.method_name());
    if (mit == sit->second.methods.end())
    {
        RpcMetrics::instance().record("server", methodKey, now - startNs, false, false, false);
        SendError(conn, reqId, traceId, kRpcFrameNotFound,
                  methodKey + " not exist");
        return;
    }

    google::protobuf::Service *service = sit->second.service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    // ---- 反序列化 ----
    std::string decompressedArgs;
    const std::string *requestArgs = &args;
    if ((header.flags() & kRpcFrameFlagCompressed) != 0)
    {
        decompressedArgs.resize(header.raw_size());
        size_t decompressedSize = 0;
        if (!DecompressRpcFrameBody(args.data(), args.size(),
                                    &decompressedArgs[0], decompressedArgs.size(),
                                    &decompressedSize) ||
            decompressedSize != header.raw_size())
        {
            RpcMetrics::instance().record("server", methodKey, now - startNs,
                                          false, false, false);
            SendError(conn, reqId, traceId, kRpcFrameBadRequest,
                      "request decompression failed");
            return;
        }
        requestArgs = &decompressedArgs;
    }

    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(method).New());
        
    if (!request->ParseFromArray(requestArgs->data(),
                                 static_cast<int>(requestArgs->size())))
    {
        RpcMetrics::instance().record("server", methodKey, now - startNs, false, false, false);
        SendError(conn, reqId, traceId, kRpcFrameBadRequest, "request parse error");
        return;
    }
    std::unique_ptr<google::protobuf::Message> response(
        service->GetResponsePrototype(method).New());

    auto *controller = new MprpcController();
    controller->setTraceId(traceId);
    if (header.deadline_ms() > 0)
    {
        // 把剩余预算写进 controller：处理过程中再发起下游 RPC 时可继续透传
        const int64_t remainMs =
            (deadlineNs - RpcProviderSteadyNowNs()) / 1000000;
        controller->SetDeadline(static_cast<uint32_t>(remainMs > 0 ? remainMs : 1));
    }

    auto *ctx = new RpcContext();
    ctx->conn = conn;
    ctx->reqId = reqId;
    ctx->traceId = traceId;
    ctx->stats = stats;
    ctx->methodKey = methodKey;
    ctx->startNs = startNs;
    ctx->deadlineNs = deadlineNs;
    ctx->request = std::move(request);
    ctx->response = std::move(response);
    ctx->controller.reset(controller);

    // ---- 在途计数 + 投递业务线程池 ----
    stats->inflight.fetch_add(1, std::memory_order_relaxed);
    RpcMetrics::instance().inflightUp("server", methodKey);

    google::protobuf::Closure *done =
        google::protobuf::NewCallback(this, &RpcProvider::OnResponse, ctx);

    const bool queued = m_workers->submit([service, method, ctx, done]()
                                          {
                                              try
                                              {
                                                  service->CallMethod(method, ctx->controller.get(),
                                                                      ctx->request.get(), ctx->response.get(), done);
                                              }
                                              catch (const std::exception &e)
                                              {
                                                  ctx->controller->SetFailed(
                                                      std::string("service handler threw: ") + e.what());
                                                  done->Run();
                                              }
                                              catch (...)
                                              {
                                                  ctx->controller->SetFailed("service handler threw");
                                                  done->Run();
                                              } });

    if (!queued)
    {
        // 队列满：回滚准入并回 429；done 仍然要 Run 一次以回收 ctx
        ctx->rejected = true;
        RpcMetrics::instance().record("server", methodKey,
                                      RpcProviderSteadyNowNs() - startNs,
                                      false, false, true);
        SendError(conn, reqId, traceId, kRpcFrameServerBusy, "worker queue full");
        done->Run();
    }
}

void RpcProvider::OnResponse(RpcContext *ctx)
{
    // 业务闭包可能在任意工作线程执行，TcpConnection::send 会负责跨线程投递
    const int64_t now = RpcProviderSteadyNowNs();
    const int64_t latency = now - ctx->startNs;

    if (!ctx->rejected)
    {
        if (ctx->deadlineNs > 0 && now > ctx->deadlineNs)
        {
            RpcMetrics::instance().record("server", ctx->methodKey, latency, false, true, false);
            char msg[128];
            snprintf(msg, sizeof(msg), "deadline exceeded (trace_id=%lu)",
                     static_cast<unsigned long>(ctx->traceId));
            SendError(ctx->conn, ctx->reqId, ctx->traceId,
                      kRpcFrameDeadlineExceeded, msg);
        }
        else if (ctx->controller->Failed())
        {
            RpcMetrics::instance().record("server", ctx->methodKey, latency, false, false, false);
            SendError(ctx->conn, ctx->reqId, ctx->traceId, kRpcFrameServerErr,
                      ctx->controller->ErrorText());
        }
        else
        {
            RpcMetrics::instance().record("server", ctx->methodKey, latency, true, false, false);
            SendResponse(ctx->conn, ctx->reqId, ctx->traceId, ctx->response.get());
        }
    }

    ctx->stats->inflight.fetch_sub(1, std::memory_order_relaxed);
    RpcMetrics::instance().inflightDown("server", ctx->methodKey);
    delete ctx;
}

void RpcProvider::SendResponse(const TcpConnectionPtr &conn, uint64_t reqId, uint64_t traceId,
                               google::protobuf::Message *response)
{
    // 先尝试压缩大响应；压缩失败或未变小时回退到原始 protobuf body
    const size_t rawSize = response->ByteSizeLong();
    if (rawSize > kRpcFrameMaxBodySize)
    {
        SendError(conn, reqId, traceId, kRpcFrameServerErr, "response too large");
        return;
    }

    RpcFrameHeader h;
    h.set_req_id(reqId);
    h.set_trace_id(traceId);

    std::string packet;

    // 大包压缩：省带宽，CPU 换网络（阈值/开关可配）
    if (m_compress && rawSize >= m_compressMin && rawSize > 0)
    {
        std::string body(rawSize, '\0');
        if (response->SerializeToArray(&body[0], static_cast<int>(rawSize)))
        {
            const size_t bound = RpcFrameCompressBound(rawSize);
            std::string comp(bound, '\0');
            size_t n = 0;
            if (CompressRpcFrameBody(body.data(), rawSize, &comp[0], bound,
                                     m_compressLevel, &n) &&
                n < rawSize)
            {
                h.set_flags(kRpcFrameFlagCompressed);
                h.set_raw_size(static_cast<uint32_t>(rawSize));
                h.set_args_size(static_cast<uint32_t>(n));
                const size_t off = BeginEncodeRpcFrame(h, n, &packet);
                if (off != kRpcFrameInvalidOffset)
                {
                    std::memcpy(&packet[off], comp.data(), n);
                    conn->send(packet);
                    return;
                }
            }
        }
        // 压缩失败 → 走未压缩路径
    }

    h.set_flags(0);
    h.set_raw_size(static_cast<uint32_t>(rawSize));
    h.set_args_size(static_cast<uint32_t>(rawSize));
    const size_t off = BeginEncodeRpcFrame(h, rawSize, &packet);
    if (off == kRpcFrameInvalidOffset)
    {
        LOG_ERROR("encode response failed, req_id=%lu", static_cast<unsigned long>(reqId));
        return;
    }
    if (!response->SerializeToArray(&packet[off], static_cast<int>(rawSize)))
    {
        LOG_ERROR("serialize response failed, req_id=%lu", static_cast<unsigned long>(reqId));
        return;
    }
    conn->send(packet);
}

void RpcProvider::SendError(const TcpConnectionPtr &conn, uint64_t reqId, uint64_t traceId,
                            int32_t errCode, const std::string &errMsg)
{
    // 错误响应不携带 body，错误文本放在 protobuf 帧头中
    RpcFrameHeader h;
    h.set_req_id(reqId);
    h.set_trace_id(traceId);
    h.set_err_code(errCode);
    h.set_err_msg(errMsg);
    h.set_args_size(0);

    std::string packet;
    const size_t off = BeginEncodeRpcFrame(h, 0, &packet);
    if (off == kRpcFrameInvalidOffset)
    {
        return;
    }
    conn->send(packet);
}

void RpcProvider::DumpMetrics()
{
    // 定时器在 IO loop 中触发，指标快照自身使用原子计数读取
    RpcMetrics::instance().dump();
}
