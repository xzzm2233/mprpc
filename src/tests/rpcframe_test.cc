#include "rpcframe.h"

#include <cassert>
#include <string>

// 验证控制帧可往返编码，且非法取消帧不会通过统一帧头校验
int main()
{
    RpcFrameHeader cancelHeader;
    cancelHeader.set_req_id(42);
    cancelHeader.set_control(kRpcFrameControlCancel);
    cancelHeader.set_args_size(0);
    cancelHeader.set_raw_size(0);

    std::string packet;
    const size_t bodyOffset = BeginEncodeRpcFrame(cancelHeader, 0, &packet);
    assert(bodyOffset != kRpcFrameInvalidOffset);

    RpcFrame parsed;
    assert(FeedRpcFrame(packet.data(), packet.size(), &parsed) ==
           RpcFrameFeedResult::kReady);
    assert(parsed.header.req_id() == 42);
    assert(parsed.header.control() == kRpcFrameControlCancel);

    RpcFrameHeader invalidCancel = cancelHeader;
    invalidCancel.set_args_size(1);
    std::string invalidPacket;
    assert(BeginEncodeRpcFrame(invalidCancel, 1, &invalidPacket) ==
           kRpcFrameInvalidOffset);

    RpcFrameHeader invalidControl = cancelHeader;
    invalidControl.set_control(2);
    assert(BeginEncodeRpcFrame(invalidControl, 0, &invalidPacket) ==
           kRpcFrameInvalidOffset);

    return 0;
}
