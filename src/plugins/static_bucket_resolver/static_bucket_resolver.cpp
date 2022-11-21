#include <algorithm>
#include <irods/filesystem/collection_iterator.hpp>
#include <irods/filesystem/filesystem.hpp>
#include <irods/filesystem/permissions.hpp>
#include <irods/user_administration.hpp>

#define BRIDGE_PLUGIN
#include "bucket_plugin.h"

#include <irods/rcConnect.h>
#include <irods/filesystem.hpp>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <string_view>
#include <nlohmann/json.hpp>

#include <fmt/core.h>
#include <dlfcn.h>

using namespace nlohmann::json_literals;

namespace
{
    std::unordered_map<std::string, std::string> buckets;
    bool resolve_bucket(rcComm_t* connection, const char* bucket, char* output, size_t* length)
    {
        fmt::print("asking for the bucket [{}]\n", bucket);
        if (auto i = buckets.find(bucket); i != buckets.end()) {
            memcpy(output, i->second.c_str(), i->second.length());
            *length = i->second.length();
            return true;
        }
        return false;
    }
    int list_buckets(rcComm_t* connection, const char* username, char*** output)
    {
        std::vector<std::string_view> matched;
        auto user = irods::experimental::administration::user(username, connection->clientUser.rodsZone);
        std::unordered_set<std::string> groups;

        {
            auto user_groups = irods::experimental::administration::client::groups(*connection, user);
            std::transform(
                user_groups.begin(), user_groups.end(), std::inserter(groups, groups.end()), [](const auto& i) {
                    return i.name;
                });
        }
        for (const auto& [_key, value] : buckets) {
            irods::experimental::filesystem::perms perm;

            auto status = irods::experimental::filesystem::client::status(*connection, value);
            for (const auto& p : status.permissions()) {
                if (p.zone != user.zone) {
                    continue;
                }
                // This needs to handle groups.
                if (p.name == username && p.prms == irods::experimental::filesystem::perms::own)
                {
                    matched.push_back(_key);
                    break;
                }
            }
        }
        *output = (char**) calloc(matched.size() + 1, sizeof(char*));
        for (int i = 0; i < matched.size(); i++) {
            (*output)[i] = strdup(matched[i].data()); // This is fine :)
        }
        return 0;
    }
} //namespace

extern "C" void plugin_initialize(rcComm_t* connection, const char* configuration)
{
    auto handle = dlopen(nullptr, RTLD_LAZY);
    auto thing = nlohmann::json::parse(configuration);
    add_bucket_plugin(&resolve_bucket, nullptr, nullptr, nullptr, &list_buckets);
    for (const auto& [k, v] : thing["mappings"].items()) {
        buckets[k] = v;
        fmt::print("Bucket [{}] set to [{}]\n", k, v);
    }
}