cmake_minimum_required(VERSION 3.13)
project(5_lab_objective)

set(CMAKE_CXX_STANDARD 14)
SET(PACKAGE "libzip")



find_package(Boost COMPONENTS system locale REQUIRED)
find_package(Boost COMPONENTS filesystem REQUIRED)
find_package(Threads REQUIRED)


FIND_PACKAGE(ZLIB REQUIRED)
INCLUDE_DIRECTORIES(${ZLIB_INCLUDE_DIR})
set(CMAKE_REQUIRED_INCLUDES ${ZLIB_INCLUDE_DIR})



include_directories(.)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(5_lab lab.cpp)


target_link_libraries(5_lab ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOCALE_LIBRARY} ${Boost_SYSTEM_LIBRARY})
target_link_libraries(5_lab Threads::Threads)
target_link_libraries(5_lab "/usr/local/opt/libzip/lib/libzip.dylib")



