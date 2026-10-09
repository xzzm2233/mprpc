#include "zookeeperutil.h"
#include "mprpcapplication.h"
#include <semaphore.h>
#include <iostream>

void global_watcher(zhandle_t *zh, int type,
                    int state, const char *path, void *watcherCtx)
{
    if (type == ZOO_SESSION_EVENT) 
    {
        if (state == ZOO_CONNECTED_STATE) 
        {
            sem_t *sem = (sem_t *)zoo_get_context(zh);
            sem_post(sem);
        }
    }
}

ZkClient::ZkClient() : m_zhandle(nullptr), m_semInited(false)
{
}

ZkClient::~ZkClient()
{
    if (m_zhandle != nullptr)
    {
        // 先关闭句柄（之后不会再触发 watcher），再销毁信号量
        zookeeper_close(m_zhandle);
        m_zhandle = nullptr;
    }
    if (m_semInited)
    {
        sem_destroy(&m_sem);
        m_semInited = false;
    }
}

void ZkClient::Start()
{
    std::string host = MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    std::string constr = host + ":" + port;

   
    m_zhandle = zookeeper_init(constr.c_str(), global_watcher, 30000, nullptr, nullptr, 0);
    if (nullptr == m_zhandle)
    {
        std::cout << "zookeeper_init error!" << std::endl;
        exit(EXIT_FAILURE);
    }

    // 信号量存为成员，避免把栈地址交给 zookeeper 句柄后在函数返回后悬空
    sem_init(&m_sem, 0, 0);
    m_semInited = true;
    zoo_set_context(m_zhandle, &m_sem);

    sem_wait(&m_sem);
    std::cout << "zookeeper_init success!" << std::endl;
}

void ZkClient::Create(const char *path, const char *data, int datalen, int state)
{
    char path_buffer[128];
    int bufferlen = sizeof(path_buffer);
    int flag;
    flag = zoo_exists(m_zhandle, path, 0, nullptr);
    if (ZNONODE == flag) 
    {
        flag = zoo_create(m_zhandle, path, data, datalen,
                          &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK)
        {
            std::cout << "znode create success... path:" << path << std::endl;
        }
        else
        {
            std::cout << "flag:" << flag << std::endl;
            std::cout << "znode create error... path:" << path << std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

std::string ZkClient::GetData(const char *path)
{
    char buffer[64];
    int bufferlen = sizeof(buffer);
    int flag = zoo_get(m_zhandle, path, 0, buffer, &bufferlen, nullptr);
    if (flag != ZOK)
    {
        std::cout << "get znode error... path:" << path << std::endl;
        return "";
    }
    else
    {
        return buffer;
    }
}