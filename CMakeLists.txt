cmake_minimum_required(VERSION 3.6)
project(pubsub)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

FIND_PACKAGE (Threads)

set(SOURCE_FILES main.cpp pubsub.hpp)
add_executable(pubsub ${SOURCE_FILES})
target_link_libraries (pubsub ${CMAKE_THREAD_LIBS_INIT})
