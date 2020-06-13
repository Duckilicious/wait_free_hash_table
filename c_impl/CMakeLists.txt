cmake_minimum_required(VERSION 3.12)
project(c_impl C)

set(CMAKE_C_STANDARD 99 )
SET(CMAKE_C_FLAGS -pthread)
add_executable(c_impl tests.c WFEXTH.c WFEXTH.h)