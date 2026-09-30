#pragma once
#include <google/protobuf/service.h>
#include <string>
#include <functional>   

class MprpcController : public google::protobuf::RpcController
{
public:
    MprpcController();
    ~MprpcController();
    void Reset();
    bool Failed() const;
    std::string ErrorText() const;
    void SetFailed(const std::string &reason);

    void StartCancel();
    bool IsCanceled() const;

    void setCancelCallback(std::function<void()> cb);

    void NotifyOnCancel(google::protobuf::Closure *callback) override;

private:
    bool m_failed;
    bool m_canceled;
    std::string m_errText;
    std::function<void()> m_cancelCallback;
};