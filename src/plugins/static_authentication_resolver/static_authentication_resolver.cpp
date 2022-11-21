#include <algorithm>
#include <irods/filesystem/collection_iterator.hpp>
#include <irods/filesystem/filesystem.hpp>
#include <irods/filesystem/permissions.hpp>
#include <irods/user_administration.hpp>

#define BRIDGE_PLUGIN
#include "auth_plugin.h"

#include <irods/rcConnect.h>
#include <irods/filesystem.hpp>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <string_view>
#include <nlohmann/json.hpp>

#include <fmt/core.h>
#include <dlfcn.h>

namespace
{
    struct user
    {
        std::string irods_username;
        std::string secret_key;
    };
    std::unordered_map<std::string, user> users;
    bool get_secret_key(rcComm_t* connection, const char* username, char* access_key)
    {
        if (users.contains(username)) {
            strcpy(access_key, users[username].secret_key.c_str());
            return true;
        }
        return false;
    }

    bool resolve_user(rcComm_t* connection, const char* username, char* irods_username)
    {
        if (users.contains(username)) {
            strcpy(irods_username, users[username].irods_username.c_str());
            return true;
        }
        return false;
    }
} //namespace

extern "C" void plugin_initialize(rcComm_t* connection, const char* configuration)
{
    auto handle = dlopen(nullptr, RTLD_LAZY);
    auto thing = nlohmann::json::parse(configuration);
    add_authentication_plugin(get_secret_key, resolve_user, nullptr, nullptr, nullptr, nullptr);
    for (const auto& [s3_username, user_info] : thing["users"].items()) {
        users[s3_username] = user{user_info["username"], user_info["secret_key"]};
    }
}