aux_source_directory(. SRC_LIST)
add_library(mprpc ${SRC_LIST})
target_compile_definitions(mprpc PRIVATE THREADED)
target_link_libraries(mprpc mymuduo pthread zookeeper_mt)