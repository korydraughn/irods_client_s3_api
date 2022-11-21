#include "./plugin.hpp"
#include <dlfcn.h>
#include <fmt/format.h>
#include "auth_plugin.h"
#include "bucket_plugin.h"

namespace irods::s3::plugins
{
    void load_plugin(rcComm_t& connection, const std::string_view& plugin_name, nlohmann::json& configuration)
    {
        auto i = fmt::format("./lib{}.so", plugin_name);
        void* plugin_handle = dlopen(i.c_str(), RTLD_LAZY | RTLD_LOCAL);
        void (*init)(rcComm_t*, const char*);

        // I love C :3
        *(void**) (&init) = dlsym(plugin_handle, "plugin_initialize");
        auto dump = configuration.dump();
        (*init)(&connection, dump.c_str());
    }
} //namespace irods::s3::plugins
