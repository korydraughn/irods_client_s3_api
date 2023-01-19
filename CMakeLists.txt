cmake_minimum_required(VERSION 3.15)

project(irods_s3_bridge CXX C)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fpermissive>)
endif()
add_subdirectory(third-party/hmac_sha256)
find_package(Boost 1.79 CONFIG REQUIRED COMPONENTS system)
find_package(fmt CONFIG REQUIRED)
find_package(IRODS 4.3.0 REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
# find_package(OpenSSL REQUIRED)
include(IrodsCXXCompiler)
include(UseLibCXX)
add_executable(irods_s3_bridge)
add_library(static_bucket_resolver SHARED)
add_library(static_authentication_resolver SHARED)
add_library(sqlite_persistence_plugin SHARED)

target_compile_definitions(irods_s3_bridge PRIVATE BOOST_ASIO_HAS_CO_AWAIT=1)
target_link_libraries(irods_s3_bridge PRIVATE 
  Boost::system 
  fmt::fmt-header-only 
  irods_client irods_plugin_dependencies 
  irods_common irods_server  
  hmac_sha256 ${CMAKE_DL_LIBS} 
  nlohmann_json::nlohmann_json)

target_include_directories(static_bucket_resolver PRIVATE src)
target_include_directories(static_authentication_resolver PRIVATE src)
target_include_directories(sqlite_persistence_plugin PRIVATE src)

target_link_libraries(static_bucket_resolver PRIVATE irods_client irods_common irods_server irods_plugin_dependencies
                      ${CMAKE_DL_LIBS} nlohmann_json::nlohmann_json fmt::fmt-header-only)
target_link_libraries(static_authentication_resolver PRIVATE irods_client irods_common irods_server irods_plugin_dependencies
                      ${CMAKE_DL_LIBS} nlohmann_json::nlohmann_json)
target_link_libraries(sqlite_persistence_plugin PRIVATE irods_client irods_common irods_server irods_plugin_dependencies
        ${CMAKE_DL_LIBS} nlohmann_json::nlohmann_json)

target_include_directories(irods_s3_bridge PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third-party/url/include third-party/hmac_sha256)
set_property(TARGET irods_s3_bridge PROPERTY CXX_STANDARD 20)
set_property(TARGET irods_s3_bridge PROPERTY ENABLE_EXPORTS ON)
set_property(TARGET static_authentication_resolver PROPERTY CXX_STANDARD 20)
set_property(TARGET static_bucket_resolver PROPERTY CXX_STANDARD 20)
set_property(TARGET sqlite_persistence_plugin PROPERTY CXX_STANDARD 20)
add_subdirectory(src)
