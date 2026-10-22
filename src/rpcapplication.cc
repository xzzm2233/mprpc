#include "rpcapplication.h"

#include <iostream>
#include <unistd.h>
#include <string>

// 框架级配置只初始化一次，供客户端和服务端组件共享
MprpcConfig MprpcApplication::m_config;

// 输出 Init() 支持的命令行格式
void ShowArgsHelp()
{
    std::cout << "format: command -i <configfile>" << std::endl;
}

void MprpcApplication::Init(int argc, char **argv)
{
    if (argc < 2)
    {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
    // getopt 解析配置文件参数；应用启动时必须显式指定配置文件
    int c = 0;
    std::string config_file;
    while ((c = getopt(argc, argv, "i:")) != -1)
    {
        switch (c)
        {
        case 'i':
            config_file = optarg;
            break;
        case '?':
            ShowArgsHelp();
            exit(EXIT_FAILURE);
        case ':':
            ShowArgsHelp();
            exit(EXIT_FAILURE);
        default:
            break;
        }
    }

    if (config_file.empty())
    {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }

    m_config.LoadConfigFile(config_file.c_str());
}

MprpcApplication &MprpcApplication::GetInstance()
{
    // C++11 保证函数局部静态对象的线程安全初始化
    static MprpcApplication app;
    return app;
}

MprpcConfig &MprpcApplication::GetConfig()
{
    return m_config;
}