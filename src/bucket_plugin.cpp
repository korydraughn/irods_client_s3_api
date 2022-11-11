#include "./bucket_plugin.h"
#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>

namespace
{
    struct bucket_plugin
    {
        bucket_creation_function create;
        bucket_resolution_function resolve;
        bucket_reversal_function reverse;
        bucket_deletion_function delete_fn;
        bucket_list_function list_fn;
    };

    std::vector<bucket_plugin> bucket_plugins;

} //namespace

extern "C" void add_bucket_plugin(
    bucket_resolution_function resolution_f,
    bucket_reversal_function reversal_f,
    bucket_creation_function creation_f,
    bucket_deletion_function deletion_f,
    bucket_list_function list_f)
{
    bucket_plugins.emplace_back(bucket_plugin{creation_f, resolution_f, reversal_f, deletion_f, list_f});
}

int create_bucket(rcComm_t* connection, const char* name)
{
    for (const auto& plugin : bucket_plugins) {
        if (plugin.create) {
            return plugin.create(connection, name);
        }
    }
    return -1;
}

int delete_bucket(rcComm_t* connection, const char* name)
{
    for (const auto& plugin : bucket_plugins) {
        if (plugin.delete_fn) {
            return plugin.delete_fn(connection, name);
        }
    }
    return -1;
}
std::vector<std::string> list_buckets(rcComm_t* connection, char*** output)
{
    std::vector<std::string> buckets;
    for (const auto& plugin : bucket_plugins) {
        if (plugin.list_fn) {
            char** current;
            plugin.list_fn(connection, &current);
            for (int i = 0; current[i] != nullptr; i++)
                buckets.push_back(current[i]);
        }
    }
    // Concatenate and turn into strings.

    return buckets;
}
