cmake_minimum_required(VERSION 3.15)

project(irods_s3_bridge CXX)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fpermissive>)
endif()

find_package(Boost 1.79 CONFIG REQUIRED COMPONENTS system)
find_package(fmt CONFIG REQUIRED)
find_package(IRODS 4.3.0 REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
include(IrodsCXXCompiler)
include(UseLibCXX)
add_executable(irods_s3_bridge)

target_compile_definitions(irods_s3_bridge PRIVATE BOOST_ASIO_HAS_CO_AWAIT=1)
target_link_libraries(irods_s3_bridge PRIVATE Boost::system fmt::fmt-header-only irods_client irods_plugin_dependencies irods_common irods_server  OpenSSL::Crypto)
target_include_directories(irods_s3_bridge PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third-party/url/include)
set_property(TARGET irods_s3_bridge PROPERTY CXX_STANDARD 20)
add_subdirectory(src)
