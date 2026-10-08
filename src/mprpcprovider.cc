#include "mprpcprovider.h"
#include "mprpcapplication.h"
#include "mprpccontroller.h"
#include "rpcheader.pb.h"

#include <mymuduo/Logger.h>
#include <zookeeperutil.h>

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo service_info;

    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();

    std::string service_name = pserviceDesc->name();

    int methodCut = pserviceDesc->method_count();

    LOG_INFO("service_name:%s:", service_name.c_str());

    for (int i = 0; i < methodCut; i++)
    {
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        service_info.m_methodMap.insert({method_name, pmethodDesc});
        LOG_INFO("method_name:%s:", method_name.c_str());
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

void RpcProvider::Run()
{
    std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    InetAddress address(port, ip);

    TcpServer server(&m_eventLoop, address, "RpcProvider");

    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1,
                                        std::placeholders::_2, std::placeholders::_3));

    server.setThreadNum(4);

    ZkClient zkCli;
    zkCli.Start();

    for (auto &sp : m_serviceMap)
    {
        std::string service_path = "/" + sp.first;
        zkCli.Create(service_path.c_str(), nullptr, 0);
        for (auto &mp : sp.second.m_methodMap)
        {
            std::string method_path = service_path + "/" + mp.first;
            char method_path_data[128] = {0};
            sprintf(method_path_data, "%s:%d", ip.c_str(), port);

            zkCli.Create(method_path.c_str(), method_path_data, strlen(method_path_data), ZOO_EPHEMERAL);
        }
    }

    LOG_INFO("RpcProvider start service at ip:%s port:%u", ip.c_str(), port);

    server.start();
    m_eventLoop.loop();
}

void RpcProvider::OnConnection(const TcpConnectionPtr &conn)
{
    if (conn->connected())
    {
        LOG_INFO("new connection: %s", conn->peerAddress().toIpPort().c_str());
    }
    else
    {
        LOG_INFO("connection closed: %s", conn->peerAddress().toIpPort().c_str());
    }
}

void RpcProvider::OnMessage(const TcpConnectionPtr &conn,
                            Buffer *buffer,
                            Timestamp time)
{
    while (true)
    {
        if (buffer->readableBytes() < 4)
        {
            return;
        }

        uint32_t header_size = 0;
        ::memcpy(&header_size, buffer->peek(), 4);

        // header_str
        if (buffer->readableBytes() < header_size + 4)
        {
            return;
        }

        std::string header_str(buffer->peek() + 4, header_size);
        mprpc::RpcHeader header;
        if (!header.ParseFromString(header_str))
        {
            LOG_ERROR("header parse error");
            buffer->retrieveAll();
            return;
        }

        uint32_t args_size = header.args_size();
        uint32_t total = 4 + header_size + args_size;
        // args_str
        if (buffer->readableBytes() < total)
        {
            return;
        }

        std::string args_str(buffer->peek() + 4 + header_size, args_size);
        buffer->retrieve(total);

        ProcessRequest(conn, header, args_str);
    }
}

void RpcProvider::ProcessRequest(const TcpConnectionPtr &conn,
                                 const mprpc::RpcHeader &header,
                                 const std::string &args_str)
{
    const std::string service_name = header.service_name();
    const std::string method_name = header.method_name();
    uint64_t req_id = header.req_id();

    auto it = m_serviceMap.find(service_name);
    if (it == m_serviceMap.end())
    {
        SendError(conn, req_id, 404, service_name + " not exist");
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if (mit == it->second.m_methodMap.end())
    {
        SendError(conn, req_id, 404, service_name + ":" + method_name + " not exist");
        return;
    }

    google::protobuf::Service *service = it->second.m_service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    google::protobuf::Message *request = service->GetRequestPrototype(method).New();
    if (!request->ParseFromString(args_str))
    {
        SendError(conn, req_id, 400, "request parse error");
        delete request;
        return;
    }

    google::protobuf::Message *response = service->GetResponsePrototype(method).New();
    MprpcController *controller = new MprpcController();

    RpcContext *ctx = new RpcContext{
        conn,
        req_id,
        std::shared_ptr<google::protobuf::Message>(response),
        std::shared_ptr<google::protobuf::Message>(request),
        std::shared_ptr<MprpcController>(controller)};

    google::protobuf::Closure *done = google::protobuf::NewCallback(
        this, &RpcProvider::OnResponse, ctx);

    service->CallMethod(method, controller, request, response, done);
}

void RpcProvider::OnResponse(RpcContext *ctx)
{
    if (ctx->controller->Failed())
    {
        SendError(ctx->conn, ctx->req_id, 500, ctx->controller->ErrorText());
    }
    else
    {
        SendResponse(ctx->conn, ctx->req_id, ctx->response.get());
    }
    delete ctx;
}

void RpcProvider::SendResponse(const TcpConnectionPtr &conn,
                               uint64_t req_id,
                               google::protobuf::Message *response)
{
    mprpc::RpcHeader header;
    header.set_req_id(req_id);
    header.set_err_code(0);
    header.set_err_msg("");

    std::string body;
    if (!response->SerializeToString(&body))
    {
        header.set_err_code(500);
        header.set_err_msg("serialize header failed!");
        body.clear();
    }
    header.set_args_size(body.size());

    std::string header_str;
    header.SerializeToString(&header_str);
    uint32_t header_size = static_cast<uint32_t>(header_str.size());

    std::string packet;
    packet.append(reinterpret_cast<char *>(&header_size), 4);
    packet.append(header_str);
    packet.append(body);

    conn->send(packet);
}

void RpcProvider::SendError(const TcpConnectionPtr &conn,
                            uint64_t req_id,
                            int32_t err_code,
                            const std::string &err_msg)
{
    mprpc::RpcHeader header;
    header.set_req_id(req_id);
    header.set_err_code(err_code);
    header.set_err_msg(err_msg);
    header.set_args_size(0);

    std::string header_str;
    header.SerializeToString(&header_str);
    uint32_t header_size = static_cast<uint32_t>(header_str.size());

    std::string packet;
    packet.append(reinterpret_cast<char *>(&header_size), 4);
    packet.append(header_str);

    conn->send(packet);
}