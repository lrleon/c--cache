cmake_minimum_required(VERSION 3.22)
project(cpp_cache)

set(CMAKE_CXX_STANDARD 20)

include_directories(./Aleph-w)
link_directories(./Aleph-w)

add_executable(cpp_cache_test cpp-cache_test.cpp)
target_link_libraries(cpp_cache_test
    PRIVATE
    Aleph
    gtest_main
    gtest
    pthread
)


add_executable(serial_mem_test serial_mem_test.cpp)

target_link_libraries(serial_mem_test
PRIVATE
    gtest_main
    gtest
    pthread
)