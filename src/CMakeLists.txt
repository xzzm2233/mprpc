# protobuf 生成源文件保持由 protoc 维护，其余条目为框架实现
set(SRC_LIST
    rpcapplication.cc
    rpcchannel.cc
    rpcconfig.cc
    rpccontroller.cc
    rpcprovider.cc
    rpcheader.pb.cc
    zookeeperutil.cc
    rpcframe.cc
    rpcmetrics.cc
    rpcthreadpool.cc)

add_library(mprpc STATIC ${SRC_LIST})

# 公共 include 和依赖随 mprpc target 传播给链接方
target_include_directories(mprpc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_compile_features(mprpc PUBLIC cxx_std_11)

# ZooKeeper 多线程客户端要求 THREADED 宏
target_compile_definitions(mprpc PUBLIC THREADED)

# 运行时依赖：网络循环、Protobuf、线程、服务发现和 Zstandard 压缩
target_link_libraries(mprpc PUBLIC mymuduo protobuf pthread zookeeper_mt zstd)

# BUILD_TESTING 开启时构建帧协议回归测试
if(BUILD_TESTING)
    add_executable(mprpc_rpcframe_test tests/rpcframe_test.cc)
    target_link_libraries(mprpc_rpcframe_test PRIVATE mprpc)
    add_test(NAME mprpc_rpcframe_test COMMAND mprpc_rpcframe_test)
endif()
