#include "mprpcprovider.h"
#include "mprpcapplication.h"
#include "rpcheader.pb.h"
#include <mymuduo/Logger.h>
#include "zookeeperutil.h"

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo service_info;
    // 获取了服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();

    int methodCut = pserviceDesc->method_count();

    LOG_INFO("service_name:%s:", service_name.c_str());

    for (int i = 0; i < methodCut; i++)
    {
        // 获取了服务对象指定下标的服务方法的描述（抽象描述）
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        service_info.m_methodMap.insert({method_name, pmethodDesc});
        LOG_INFO("method_name:%s:", method_name.c_str());
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务节点，开始提供rpc远程网络调用服务
void RpcProvider::Run()
{
    // 读取配置文件rpcservice的信息
    std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    InetAddress address( port,ip);

    // 创建TcpServer对象
    TcpServer server(&m_eventLoop, address, "RpcProvider");

    // 绑定连接回调和消息读写回调方法 分离了网络代码与业务代码
    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1,
                                        std::placeholders::_2, std::placeholders::_3));

    // 设置muduo库的线程数量
    server.setThreadNum(4);

    ZkClient zkCli;
    zkCli.Start();

    for (auto &sp : m_serviceMap)
    {
        // /server_name  /UserServiceRpc
        std::string service_path = "/" + sp.first;
        zkCli.Create(service_path.c_str(), nullptr, 0);
        for (auto &mp : sp.second.m_methodMap)
        {
            std::string method_path = service_path + "/" + mp.first;
            char method_path_data[128] = {0};
            sprintf(method_path_data, "%s:%d", ip.c_str(), port);
            // ZOO_EPHEMERAL表示znode是一个临时性节点
            zkCli.Create(method_path.c_str(), method_path_data, strlen(method_path_data), ZOO_EPHEMERAL);
        }
    }

    LOG_INFO("RpcProvider start service at ip:%s port:%u", ip.c_str(), port);

    // 启动网络服务
    server.start();
    m_eventLoop.loop();
}
// 新的socket连接回调
void RpcProvider::OnConnection(const TcpConnectionPtr &conn)
{
    if (!conn->connected())
    {
        // 和rpc client的连接断开了
        conn->shutdown();
    }
}

void RpcProvider::OnMessage(const TcpConnectionPtr &conn,
                            Buffer *buffer,
                            Timestamp)
{
    // 网络上接收的远程rpc调用请求的字符流
    std::string recv_buf = buffer->retrieveAllAsString();

    // 从字符流中读取前四个字节的内容
    uint32_t header_size = 0;
    recv_buf.copy((char *)&header_size, 4, 0);

    // 根据header_size读取数据头的原始字符流，反序列化数据，得到rpc请求的详细信息
    std::string rpc_header_str = recv_buf.substr(4, header_size);
    mprpc::RpcHeader rpcHeader;
    std::string service_name;
    std::string method_name;
    uint32_t args_size;
    if (rpcHeader.ParseFromString(rpc_header_str))
    {
        // 数据头反序列化成功
        service_name = rpcHeader.service_name();
        method_name = rpcHeader.method_name();
        args_size = rpcHeader.args_size();
    }
    else
    {
        // 数据头反序列化失败
        // std::cout << "rpc_header_str:" << rpc_header_str << " parse error!" << std::endl;
        LOG_ERROR("rpc_header_str:%s parse error!", rpc_header_str.c_str());
        return;
    }

    // 获取rpc方法参数的字符流数据
    std::string args_str = recv_buf.substr(4 + header_size, args_size);
    // 获取service对象和method对象
    auto it = m_serviceMap.find(service_name);
    if (it == m_serviceMap.end())
    {
        // std::cout << service_name << " is not exist!" << std::endl;
        LOG_INFO("%s is not exist!", service_name.c_str());
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if (mit == it->second.m_methodMap.end())
    {
        // std::cout << service_name << ":" << method_name << " is not exist!" << std::endl;
        LOG_ERROR("%s:%s is not exist!", service_name.c_str(), method_name.c_str());
        return;
    }
    google::protobuf::Service *service = it->second.m_service;      // 获取service对象
    const google::protobuf::MethodDescriptor *method = mit->second; // 获取method对象

    // 生成rpc方法调用的请求request和响应response参数
    google::protobuf::Message *request = service->GetRequestPrototype(method).New();
    if (!request->ParseFromString(args_str))
    {
        // std::cout << "request parse error! content:" << args_str << std::endl;
        LOG_ERROR("request parse error! content:%s", args_str.c_str());
        return;
    }
    google::protobuf::Message *response = service->GetResponsePrototype(method).New();

    // 给下面的method方法的调用，绑定一个Closure的回调函数
    google::protobuf::Closure *done = google::protobuf::NewCallback<RpcProvider, const TcpConnectionPtr &, google::protobuf::Message *>(this, &RpcProvider::SendrpcResponse, conn, response);

    // 在框架上根据远端rpc请求，调用当前rpc节点上发布的方法
    // new UserService().Login(controller, request, response, done)
    service->CallMethod(method, nullptr, request, response, done);
}

// Closure的回调操作，用于序列化rpc的响应和网络发送
void RpcProvider::SendrpcResponse(const TcpConnectionPtr &conn, google::protobuf::Message *response)
{
    std::string response_str;
    if (response->SerializeToString(&response_str)) // response进行序列化
    {
        // 序列化成功后，通过网络把rpc方法的执行结果发送回rpc的调用方
        conn->send(response_str);
    }
    else
    {
        // std::cout << "serialize response_str error!" << std::endl;
        LOG_ERROR("serialize response_str error!");
    }
    conn->shutdown(); // 模拟http的短链接服务，由rpcprovider主动断开连接
}