target_sources(irods_s3_bridge PRIVATE main.cpp connection.cpp persistence_plugin.cpp
               event_loop.cpp hmac.cpp #bucket.cpp
               authentication.cpp bucket_plugin.cpp
               auth_plugin.cpp
               plugin.cpp)
add_subdirectory(plugins)
add_subdirectory(s3)
