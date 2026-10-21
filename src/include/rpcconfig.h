#pragma once
#include <unordered_map>
#include <string>

class MprpcConfig
{
public:
    // 每次加载会替换现有配置，同一文件内重复键以后出现的值为准。
    void LoadConfigFile(const char *config_file);

    // 读取配置，key 不存在时打一条 error 日志并返回空串
    std::string Load(const std::string &key);

    // 读取可选配置：key 不存在/为空时返回 def，不刷日志
    std::string Load(const std::string &key, const std::string &def);

    // 配置缺失、空值或格式/范围非法时返回调用方给定的默认值。
    int LoadInt(const std::string &key, int def);
    // 支持 1/0、true/false、yes/no、on/off，忽略字母大小写。
    bool LoadBool(const std::string &key, bool def);
    double LoadDouble(const std::string &key, double def);

private:
    // 配置项按键保存；重复键以后出现的值为准。
    std::unordered_map<std::string, std::string> m_configMap;
    // 去掉字符串前后的空格
    void Trim(std::string &src_buf);
};
