cmake_minimum_required(VERSION 3.15)

project(irods_s3_bridge CXX)


find_package(Boost 1.79 CONFIG REQUIRED COMPONENTS system)
find_package(fmt CONFIG REQUIRED)
find_package(IRODS 4.3.0 REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
include(IrodsCXXCompiler)
add_executable(irods_s3_bridge)
target_link_libraries(irods_s3_bridge PRIVATE Boost::system fmt::fmt irods_client irods_plugin_dependencies irods_common  OpenSSL::Crypto)
target_include_directories(irods_s3_bridge PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third-party/url/include)
set_property(TARGET irods_s3_bridge PROPERTY CXX_STANDARD 20)
add_subdirectory(src)
