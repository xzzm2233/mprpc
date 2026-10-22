#pragma once

#include "rpcheader.pb.h"

#include <cstddef>
#include <cstdint>
#include <string>

// 单帧限制：解析网络数据前先校验，避免异常长度导致过量分配
const uint32_t kRpcFrameMaxHeaderSize = 1u * 1024 * 1024;
const uint32_t kRpcFrameMaxBodySize = 64u * 1024 * 1024;
const uint32_t kRpcFrameFlagCompressed = 0x0001; // body 使用 Zstandard 压缩
const size_t kRpcFrameInvalidOffset = static_cast<size_t>(-1);

// 与 RpcHeader.err_code 共用的 RPC 错误码
enum RpcFrameErrCode : int32_t
{
    kRpcFrameOk = 0,
    kRpcFrameBadRequest = 400,
    kRpcFrameNotFound = 404,
    kRpcFrameCanceled = 499,
    kRpcFrameServerBusy = 429,
    kRpcFrameDeadlineExceeded = 408,
    kRpcFrameServerErr = 500,
};

typedef mprpc::RpcHeader RpcFrameHeader;

// 解码 protobuf 帧头，并校验长度、压缩标记和 body 大小
bool DecodeRpcFrameHeader(const char *data, size_t len, RpcFrameHeader *header);

// 写入 4 字节大端帧头长度、protobuf 帧头及预留 body 空间
// 返回 body 写入偏移，参数非法时返回 kRpcFrameInvalidOffset
size_t BeginEncodeRpcFrame(const RpcFrameHeader &header, size_t bodyLen,
                           std::string *out);

// 增量解析结果：完整帧、需要更多字节或遇到非法数据
enum class RpcFrameFeedResult
{
    kNeedMore,
    kReady,
    kError,
};

struct RpcFrame
{
    // 每条 TCP 连接持有一个解析状态，按长度前缀、帧头、body 依次推进
    enum Stage
    {
        kSize,
        kHeader,
        kBody
    };

    Stage stage = kSize;
    uint32_t headerSize = 0;
    RpcFrameHeader header;
    int64_t startNs = 0; // 首字节到达时的 steady_clock 纳秒时间

    // 丢弃当前帧进度，准备解析下一帧。
    void reset() { *this = RpcFrame(); }

    // body 在当前可读缓冲区中的起始地址
    const char *body(const char *readableBase) const
    {
        return readableBase + 4 + headerSize;
    }
    size_t totalSize() const
    {
        return 4u + headerSize + header.args_size();
    }
};

// 从 readable 字节流中尝试解析一帧；data 必须从当前帧起始位置开始
RpcFrameFeedResult FeedRpcFrame(const char *data, size_t readable,
                                RpcFrame *frame);

// Zstandard body 压缩/解压辅助函数
size_t RpcFrameCompressBound(size_t srcLen);
bool CompressRpcFrameBody(const void *src, size_t srcLen, void *dst,
                          size_t dstCap, int level, size_t *outLen);
bool DecompressRpcFrameBody(const void *src, size_t srcLen, void *dst,
                            size_t dstCap, size_t *outLen);
