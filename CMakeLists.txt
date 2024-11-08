cmake_minimum_required(VERSION 3.29)
project(cpp_cache)

set(CMAKE_CXX_STANDARD 23)

include_directories(./Aleph-w)

add_executable(cpp_cache_test cpp-cache_test.cpp)