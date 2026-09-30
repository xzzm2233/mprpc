#include "mprpccontroller.h"

MprpcController::MprpcController()
    : m_failed(false),
      m_canceled(false),
      m_errText("")
{
}

MprpcController::~MprpcController()
{
}

void MprpcController::Reset()
{
    m_failed = false;
    m_canceled = false;
    m_errText = "";
    m_cancelCallback = nullptr;
}

bool MprpcController::Failed() const
{
    return m_failed;
}

std::string MprpcController::ErrorText() const
{
    return m_errText;
}

void MprpcController::SetFailed(const std::string &reason)
{
    m_failed = true;
    m_errText = reason;
}

void MprpcController::StartCancel()
{
    m_canceled = true;
    if (m_cancelCallback)
    {
        m_cancelCallback();
    }
}

bool MprpcController::IsCanceled() const
{
    return m_canceled;
}

void MprpcController::setCancelCallback(std::function<void()> cb)
{
    m_cancelCallback = std::move(cb);
}

void MprpcController::NotifyOnCancel(google::protobuf::Closure *callback)
{
    (void)callback;
}