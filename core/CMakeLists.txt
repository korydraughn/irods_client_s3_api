include(GetGitRevisionDescription)
get_git_head_revision(IRODS_S3_API_GIT_REFSPEC IRODS_S3_API_GIT_SHA1)

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/s3_api/version.hpp.in"
  "${CMAKE_CURRENT_BINARY_DIR}/include/irods/private/s3_api/version.hpp"
  @ONLY
)

add_library(
  irods_s3_api_core
  OBJECT
  "${CMAKE_CURRENT_SOURCE_DIR}/src/common.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/crlf_parser.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/globals.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/process_stash.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/session.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/transport.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/connection.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/configuration.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/authentication.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/auth_plugin.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/bucket_plugin.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/hmac.cpp"
)

target_link_libraries(
  irods_s3_api_core
  PRIVATE
  irods_client
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_url.so"
  CURL::libcurl
  hmac_sha256
)

target_compile_definitions(
  irods_s3_api_core
  PRIVATE
  ${IRODS_COMPILE_DEFINITIONS}
  ${IRODS_COMPILE_DEFINITIONS_PRIVATE}
  # The following macro requires that the application never attempt to
  # change log levels concurrently across threads.
  SPDLOG_NO_ATOMIC_LEVELS
  # Defines a macro which is used to construct the URL paths to individual
  # endpoint handlers (e.g. /irods-s3-api/<version>/collections).
  IRODS_S3_API_BASE_URL="/irods-s3-api/${IRODS_S3_API_VERSION}"
)

target_include_directories(
  irods_s3_api_core
  PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
  "${CMAKE_CURRENT_BINARY_DIR}/include"
  "${IRODS_S3_API_PROJECT_SOURCE_DIR}/endpoints/shared/include"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
)

if (IRODS_VERSION VERSION_GREATER "4.3.1")
  target_link_libraries(
    irods_s3_api_core
    PUBLIC
    fmt::fmt
    spdlog::spdlog
  )
else()
  target_link_libraries(
    irods_s3_api_core
    PRIVATE
    "${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so"
  )

  target_include_directories(
    irods_s3_api_core
    PRIVATE
    "${IRODS_EXTERNALS_FULLPATH_FMT}/include"
    "${IRODS_EXTERNALS_FULLPATH_SPDLOG}/include"
  )
endif()

set_target_properties(irods_s3_api_core PROPERTIES EXCLUDE_FROM_ALL TRUE)
