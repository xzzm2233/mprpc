#include "mprpcconfig.h"

#include <mymuduo/Logger.h>
#include <string>

void MprpcConfig::LoadConfigFile(const char *config_file)
{
    FILE *pf = fopen(config_file, "r");
    if (!pf)
    {
        LOG_ERROR("%s is not exist!", config_file);
        exit(EXIT_FAILURE);
    }

    char buf[512];
    while (fgets(buf, sizeof buf, pf) != nullptr)
    {
        std::string read_buf(buf);
        Trim(read_buf);

        if (read_buf.empty() || read_buf[0] == '#')
        {
            continue;
        }

        size_t idx = read_buf.find('=');
        if (idx == std::string::npos)
        {
            continue;
        }

        std::string key = read_buf.substr(0, idx);
        std::string value = read_buf.substr(idx + 1);
        Trim(key);
        Trim(value);

        if (key.empty())
        {
            continue;
        }

        m_configMap.insert({key, value});
    }

    fclose(pf);
}

std::string MprpcConfig::Load(const std::string &key)
{
    auto it = m_configMap.find(key);
    if (it == m_configMap.end())
    {
        LOG_ERROR("config key [%s] not found", key.c_str());
        return "";
    }
    return it->second;
}

void MprpcConfig::Trim(std::string &src_buf)
{
    static const char *kWhite = " \t\n\r\f\v";
    size_t idx = src_buf.find_first_not_of(kWhite);
    if (idx == std::string::npos) // size_t<-1>
    {
        src_buf.clear();
        return;
    }
    src_buf.erase(0, idx);

    idx = src_buf.find_last_not_of(kWhite);
    src_buf.erase(idx + 1);
}