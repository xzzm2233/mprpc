#include "rpcframe.h"

#include <zstd.h>

#include <chrono>
#include <cstring>
#include <limits>

// 验证 protobuf 帧头字段是否合法
static bool IsValidRpcFrameHeader(const RpcFrameHeader &header)
{
    if (header.args_size() > kRpcFrameMaxBodySize ||
        header.raw_size() > kRpcFrameMaxBodySize ||
        (header.flags() & ~kRpcFrameFlagCompressed) != 0)
    {
        return false;
    }

    if ((header.flags() & kRpcFrameFlagCompressed) != 0)
    {
        return header.raw_size() != 0 && header.args_size() != 0;
    }

    return header.raw_size() == 0 || header.raw_size() == header.args_size();
}

bool DecodeRpcFrameHeader(const char *data, size_t len,
                          RpcFrameHeader *header)
{
    // ParseFromArray 接受 int 长度，先限制范围再转换
    if (data == nullptr || header == nullptr || len == 0 ||
        len > kRpcFrameMaxHeaderSize ||
        len > static_cast<size_t>(std::numeric_limits<int>::max()))
    {
        return false;
    }
    if (!header->ParseFromArray(data, static_cast<int>(len)))
    {
        return false;
    }
    return IsValidRpcFrameHeader(*header);
}

size_t BeginEncodeRpcFrame(const RpcFrameHeader &header, size_t bodyLen,
                           std::string *out)
{
    // args_size 必须与实际 body 长度一致，避免帧长度字段和数据不符
    if (out == nullptr || bodyLen > kRpcFrameMaxBodySize ||
        bodyLen > std::numeric_limits<uint32_t>::max() ||
        header.args_size() != bodyLen || !IsValidRpcFrameHeader(header))
    {
        return kRpcFrameInvalidOffset;
    }

    std::string encodedHeader;
    if (!header.SerializeToString(&encodedHeader) ||
        encodedHeader.empty() || encodedHeader.size() > kRpcFrameMaxHeaderSize)
    {
        return kRpcFrameInvalidOffset;
    }

    out->resize(4 + encodedHeader.size() + bodyLen);
    const uint32_t headerSize = static_cast<uint32_t>(encodedHeader.size());
    (*out)[0] = static_cast<char>(headerSize >> 24);
    (*out)[1] = static_cast<char>(headerSize >> 16);
    (*out)[2] = static_cast<char>(headerSize >> 8);
    (*out)[3] = static_cast<char>(headerSize);
    std::memcpy(&(*out)[4], encodedHeader.data(), encodedHeader.size());
    return 4 + encodedHeader.size();
}

RpcFrameFeedResult FeedRpcFrame(const char *data, size_t readable,
                                RpcFrame *frame)
{
    // 每次调用都从当前缓冲区起点继续推进保存的解析阶段
    if (data == nullptr || frame == nullptr)
    {
        return RpcFrameFeedResult::kError;
    }

    if (frame->stage == RpcFrame::kSize)
    {
        if (readable < 4)
        {
            return RpcFrameFeedResult::kNeedMore;
        }
        frame->headerSize =
            (static_cast<uint32_t>(static_cast<unsigned char>(data[0])) << 24) |
            (static_cast<uint32_t>(static_cast<unsigned char>(data[1])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(data[2])) << 8) |
            static_cast<uint32_t>(static_cast<unsigned char>(data[3]));
        if (frame->headerSize == 0 ||
            frame->headerSize > kRpcFrameMaxHeaderSize)
        {
            return RpcFrameFeedResult::kError;
        }
        frame->startNs = static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
        frame->stage = RpcFrame::kHeader;
    }

    if (frame->stage == RpcFrame::kHeader)
    {
        if (readable < 4u + frame->headerSize)
        {
            return RpcFrameFeedResult::kNeedMore;
        }
        if (!DecodeRpcFrameHeader(data + 4, frame->headerSize, &frame->header))
        {
            return RpcFrameFeedResult::kError;
        }
        frame->stage = RpcFrame::kBody;
    }

    if (readable < frame->totalSize())
    {
        return RpcFrameFeedResult::kNeedMore;
    }
    return RpcFrameFeedResult::kReady;
}

size_t RpcFrameCompressBound(size_t srcLen)
{
    return ZSTD_compressBound(srcLen);
}

bool CompressRpcFrameBody(const void *src, size_t srcLen, void *dst,
                          size_t dstCap, int level, size_t *outLen)
{
    // Zstandard 错误码通过 ZSTD_isError 识别，不直接当作有效长度返回
    if (outLen == nullptr)
    {
        return false;
    }
    const size_t n = ZSTD_compress(dst, dstCap, src, srcLen, level);
    if (ZSTD_isError(n))
    {
        return false;
    }
    *outLen = n;
    return true;
}

bool DecompressRpcFrameBody(const void *src, size_t srcLen, void *dst,
                            size_t dstCap, size_t *outLen)
{
    // 调用方负责按帧头声明的 raw_size 提供有界目标缓冲区
    if (outLen == nullptr)
    {
        return false;
    }
    const size_t n = ZSTD_decompress(dst, dstCap, src, srcLen);
    if (ZSTD_isError(n))
    {
        return false;
    }
    *outLen = n;
    return true;
}
