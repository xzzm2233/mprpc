set(SRC_LIST
    mprpcapplication.cc
    mprpcchannel.cc
    mprpcconfig.cc
    mprpccontroller.cc
    mprpcprovider.cc
    rpcheader.pb.cc
    zookeeperutil.cc)

add_library(mprpc ${SRC_LIST})

target_compile_definitions(mprpc PRIVATE THREADED)

target_link_libraries(mprpc mymuduo pthread zookeeper_mt)