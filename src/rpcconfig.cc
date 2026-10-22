#include "rpcconfig.h"

#include <mymuduo/Logger.h>

#include <cerrno>
#include <cctype>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <string>

// 逐行读取 key=value 配置，忽略空行和注释行
void MprpcConfig::LoadConfigFile(const char *config_file)
{
    if (config_file == nullptr)
    {
        LOG_ERROR("config file path is null");
        exit(EXIT_FAILURE);
    }

    std::ifstream input(config_file);
    if (!input)
    {
        LOG_ERROR("failed to open config file: %s", config_file);
        exit(EXIT_FAILURE);
    }

    // 每次加载都以新文件为准；同一文件内重复键由后出现的值覆盖
    m_configMap.clear();

    std::string read_buf;
    size_t lineNumber = 0;
    while (std::getline(input, read_buf))
    {
        ++lineNumber;
        Trim(read_buf);

        if (read_buf.empty() || read_buf[0] == '#')
        {
            continue;
        }

        size_t idx = read_buf.find('=');
        if (idx == std::string::npos)
        {
            LOG_ERROR("invalid config line %lu in %s: missing '='",
                      static_cast<unsigned long>(lineNumber), config_file);
            continue;
        }

        std::string key = read_buf.substr(0, idx);
        std::string value = read_buf.substr(idx + 1);
        Trim(key);
        Trim(value);

        if (key.empty())
        {
            LOG_ERROR("invalid config line %lu in %s: empty key",
                      static_cast<unsigned long>(lineNumber), config_file);
            continue;
        }

        m_configMap[key] = value;
    }

    if (input.bad())
    {
        LOG_ERROR("failed while reading config file: %s", config_file);
        exit(EXIT_FAILURE);
    }
}

std::string MprpcConfig::Load(const std::string &key)
{
    // 必选项缺失会记录错误，调用方仍负责校验返回值
    auto it = m_configMap.find(key);
    if (it == m_configMap.end())
    {
        LOG_ERROR("config key [%s] not found", key.c_str());
        return "";
    }
    return it->second;
}

std::string MprpcConfig::Load(const std::string &key, const std::string &def)
{
    // 可选项缺失或为空时采用默认值
    auto it = m_configMap.find(key);
    if (it == m_configMap.end() || it->second.empty())
    {
        return def;
    }
    return it->second;
}

int MprpcConfig::LoadInt(const std::string &key, int def)
{
    auto it = m_configMap.find(key);
    if (it == m_configMap.end() || it->second.empty())
    {
        return def;
    }

    errno = 0;
    char *end = nullptr;
    const long value = std::strtol(it->second.c_str(), &end, 10);
    if (errno == ERANGE || end == it->second.c_str() || *end != '\0' ||
        value < INT_MIN || value > INT_MAX)
    {
        LOG_ERROR("invalid integer config [%s]=[%s], using default",
                  key.c_str(), it->second.c_str());
        return def;
    }
    return static_cast<int>(value);
}

bool MprpcConfig::LoadBool(const std::string &key, bool def)
{
    // 接受常见的 true/false 写法，未知值回退到默认值
    auto it = m_configMap.find(key);
    if (it == m_configMap.end() || it->second.empty())
    {
        return def;
    }
    std::string value = it->second;
    for (std::string::iterator c = value.begin(); c != value.end(); ++c)
    {
        *c = static_cast<char>(
            std::tolower(static_cast<unsigned char>(*c)));
    }

    if (value == "1" || value == "true" || value == "yes" || value == "on")
    {
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off")
    {
        return false;
    }
    LOG_ERROR("invalid boolean config [%s]=[%s], using default",
              key.c_str(), it->second.c_str());
    return def;
}

double MprpcConfig::LoadDouble(const std::string &key, double def)
{
    auto it = m_configMap.find(key);
    if (it == m_configMap.end() || it->second.empty())
    {
        return def;
    }
    errno = 0;
    char *end = nullptr;
    const double value = std::strtod(it->second.c_str(), &end);
    if (errno == ERANGE || end == it->second.c_str() || *end != '\0' ||
        !std::isfinite(value))
    {
        LOG_ERROR("invalid floating-point config [%s]=[%s], using default",
                  key.c_str(), it->second.c_str());
        return def;
    }
    return value;
}

void MprpcConfig::Trim(std::string &src_buf)
{
    // 移除字符串两端的空白字符，不改变中间内容
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