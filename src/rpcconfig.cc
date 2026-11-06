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

    std::string configLine;
    size_t lineNumber = 0;
    while (std::getline(input, configLine))
    {
        ++lineNumber;
        Trim(configLine);

        if (configLine.empty() || configLine[0] == '#')
        {
            continue;
        }

        const size_t separatorPosition = configLine.find('=');
        if (separatorPosition == std::string::npos)
        {
            LOG_ERROR("invalid config line %lu in %s: missing '='",
                      static_cast<unsigned long>(lineNumber), config_file);
            continue;
        }

        std::string key = configLine.substr(0, separatorPosition);
        std::string value = configLine.substr(separatorPosition + 1);
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
    auto configEntry = m_configMap.find(key);
    if (configEntry == m_configMap.end())
    {
        LOG_ERROR("config key [%s] not found", key.c_str());
        return "";
    }
    return configEntry->second;
}

std::string MprpcConfig::Load(const std::string &key, const std::string &def)
{
    // 可选项缺失或为空时采用默认值
    auto configEntry = m_configMap.find(key);
    if (configEntry == m_configMap.end() || configEntry->second.empty())
    {
        return def;
    }
    return configEntry->second;
}

int MprpcConfig::LoadInt(const std::string &key, int def)
{
    auto configEntry = m_configMap.find(key);
    if (configEntry == m_configMap.end() || configEntry->second.empty())
    {
        return def;
    }

    errno = 0;
    char *end = nullptr;
    const long value = std::strtol(configEntry->second.c_str(), &end, 10);
    if (errno == ERANGE || end == configEntry->second.c_str() || *end != '\0' ||
        value < INT_MIN || value > INT_MAX)
    {
        LOG_ERROR("invalid integer config [%s]=[%s], using default",
                  key.c_str(), configEntry->second.c_str());
        return def;
    }
    return static_cast<int>(value);
}

bool MprpcConfig::LoadBool(const std::string &key, bool def)
{
    // 接受常见的 true/false 写法，未知值回退到默认值
    auto configEntry = m_configMap.find(key);
    if (configEntry == m_configMap.end() || configEntry->second.empty())
    {
        return def;
    }
    std::string value = configEntry->second;
    for (std::string::iterator character = value.begin(); character != value.end(); ++character)
    {
        *character = static_cast<char>(
            std::tolower(static_cast<unsigned char>(*character)));
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
              key.c_str(), configEntry->second.c_str());
    return def;
}

double MprpcConfig::LoadDouble(const std::string &key, double def)
{
    auto configEntry = m_configMap.find(key);
    if (configEntry == m_configMap.end() || configEntry->second.empty())
    {
        return def;
    }
    errno = 0;
    char *end = nullptr;
    const double value = std::strtod(configEntry->second.c_str(), &end);
    if (errno == ERANGE || end == configEntry->second.c_str() || *end != '\0' ||
        !std::isfinite(value))
    {
        LOG_ERROR("invalid floating-point config [%s]=[%s], using default",
                  key.c_str(), configEntry->second.c_str());
        return def;
    }
    return value;
}

void MprpcConfig::Trim(std::string &text)
{
    // 移除字符串两端的空白字符，不改变中间内容
    static const char *kWhite = " \t\n\r\f\v";
    size_t whitespacePosition = text.find_first_not_of(kWhite);
    if (whitespacePosition == std::string::npos) // size_t<-1>
    {
        text.clear();
        return;
    }
    text.erase(0, whitespacePosition);

    whitespacePosition = text.find_last_not_of(kWhite);
    text.erase(whitespacePosition + 1);
}