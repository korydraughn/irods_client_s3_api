cmake_minimum_required(VERSION 3.15)

project(irods_s3_bridge CXX)


find_package(Boost REQUIRED COMPONENTS system)
find_package(fmt REQUIRED)
add_executable(irods_s3_bridge)
target_link_libraries(irods_s3_bridge Boost::system fmt::fmt)
set_property(TARGET irods_s3_bridge PROPERTY CXX_STANDARD 20)
add_subdirectory(src)
