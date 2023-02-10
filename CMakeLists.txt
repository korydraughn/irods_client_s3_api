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
install(TARGETS irods_s3_bridge static_authentication_resolver static_bucket_resolver)


set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENT_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENTS_GROUPING IGNORE)
set(CPACK_PACKAGE_VERSION ${IRODS_CLIENT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The integrated Rule-Oriented Data System")


if (IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "ubuntu" OR IRODS_LINUX_DISTRIBUTION_NAME STREQUAL "debian")
  if (NOT IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME)
    execute_process(
            COMMAND "lsb_release" "-s" "-c"
            RESULT_VARIABLE IRODS_PLUGIN_EXECUTE_PROCESS_RESULT_LINUX_DISTRIBUTION_VERSION_CODENAME
            OUTPUT_VARIABLE IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME
    )
    string(STRIP ${IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME} IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME)
    if (NOT ${IRODS_PLUGIN_EXECUTE_PROCESS_RESULT_LINUX_DISTRIBUTION_VERSION_CODENAME} STREQUAL "0")
      message(FATAL_ERROR "Linux lsb_release shortname detection failed\n${IRODS_PLUGIN_EXECUTE_PROCESS_RESULT_LINUX_DISTRIBUTION_VERSION_CODENAME}")
    endif()
    set(IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME ${IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME} CACHE STRING "Linux distribution version codename, e.g. {precise, wheezy, trusty, jessie, ...}." FORCE)
    message(STATUS "Setting unspecified IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME to '${IRODS_PLUGIN_LINUX_DISTRIBUTION_VERSION_CODENAME}'")
  endif()
endif()

if (NOT CPACK_GENERATOR)
  set(CPACK_GENERATOR ${IRODS_CPACK_GENERATOR} CACHE STRING "CPack generator to use, e.g. {DEB, RPM, TGZ}." FORCE)
  message(STATUS "Setting unspecified CPACK_GENERATOR to ${CPACK_GENERATOR}. This is the correct setting for normal builds.")
endif()

include(IrodsCPackCommon)

set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION ON)
set(CPACK_DEBIAN_PACKAGE_RELEASE "${IRODS_PACKAGE_REVISION}")

set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/usr/sbin")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${CMAKE_INSTALL_SYSCONFDIR}")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts/irods")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts/irods/test")

if (IRODS_LINUX_DISTRIBUTION_VERSION_CODENAME)
  set(CPACK_DEBIAN_PACKAGE_RELEASE "${CPACK_DEBIAN_PACKAGE_RELEASE}~${IRODS_LINUX_DISTRIBUTION_VERSION_CODENAME}")
endif()
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PACKAGE_NAME "${PROJECT_NAME_HYPHENS}")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "${IRODS_PACKAGE_DEPENDENCIES_STRING}, irods-runtime (= ${IRODS_VERSION}), libc6" )
add_subdirectory(src)

include(CPack)