#pragma once
#include "rpcconfig.h"

// RPC 框架入口：负责初始化配置，并提供进程级配置访问
class MprpcApplication
{
public:
    // 从命令行读取配置文件路径（-i <configfile>）并加载配置
    static void Init(int argc, char **argv);

    // 返回不可复制的进程级应用实例
    static MprpcApplication &GetInstance();

    // 返回 Init() 加载的全局配置对象
    static MprpcConfig &GetConfig();

private:
    static MprpcConfig m_config;
    MprpcApplication() {}
    
    MprpcApplication(const MprpcApplication &) = delete;
    MprpcApplication(MprpcApplication &&) = delete;
};
