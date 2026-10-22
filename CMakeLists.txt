cmake_minimum_required(VERSION 3.10)
project(mprpc)

# 默认使用 C++11
if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11 CACHE STRING "C++ standard used to build mprpc")
endif()
if(CMAKE_CXX_STANDARD LESS 11)
    message(FATAL_ERROR "mprpc requires C++11 or newer")
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 以源码方式集成 mymuduo，其 PUBLIC include 目录会通过 target_link_libraries 自动传播
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../mymuduo
    ${CMAKE_CURRENT_SOURCE_DIR}/build_mymuduo)

add_subdirectory(src)
