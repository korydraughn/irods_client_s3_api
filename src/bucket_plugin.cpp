#include "./bucket_plugin.h"
#include "./bucket.hpp"

#include <stdexcept>
#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>

#include <boost/url.hpp>

#include <irods/irods_exception.hpp>
#include <irods/filesystem.hpp>

namespace fs = irods::experimental::filesystem;

namespace
{
    struct bucket_plugin
    {
        bucket_creation_function create = nullptr;
        bucket_resolution_function resolve = nullptr;
        bucket_reversal_function reverse = nullptr;
        bucket_deletion_function delete_fn = nullptr;
        bucket_list_function list_fn = nullptr;
    };

    bucket_plugin active_bucket_plugin;

} //namespace

extern "C" void add_bucket_plugin(
    bucket_resolution_function resolution_f,
    bucket_reversal_function reversal_f,
    bucket_creation_function creation_f,
    bucket_deletion_function deletion_f,
    bucket_list_function list_f)
{
    active_bucket_plugin = bucket_plugin{creation_f, resolution_f, reversal_f, deletion_f, list_f};
}

int create_bucket(rcComm_t* connection, const char* name)
{
    if (active_bucket_plugin.create) {
        return active_bucket_plugin.create(connection, name);
    }
    return -1;
}

int delete_bucket(rcComm_t* connection, const char* name)
{
    if (active_bucket_plugin.delete_fn) {
        return active_bucket_plugin.delete_fn(connection, name);
    }
    return -1;
}

std::vector<std::string> list_buckets(rcComm_t* connection, const char* username, char*** output)
{
    std::vector<std::string> buckets;

    if (active_bucket_plugin.list_fn) {
        char** items;
        auto result = active_bucket_plugin.list_fn(connection, username, &items);
        if (result != 0) {
            throw irods::exception(result, "Error in list_buckets", __FILE__, __LINE__, __FUNCTION__);
        }
        for (int i = 0; items[i] != nullptr; i++) {
            buckets.emplace_back(items[i]);
            free(items[i]); // This might not be enough to work for every purpose.
            // It might be necessary to provide a destructor if delete is incompatible with it. :p
        }
        free(items);
    }
    return buckets;
}

/// Produces the basic irods path of the bucket. This will need concatenation with the remainder of the key.
std::optional<fs::path> irods::s3::resolve_bucket(rcComm_t& connection, const boost::urls::segments_view& view)
{
    char buffer[300];
    memset(buffer, 0, 300);
    size_t pathlen = 0;
    int result = active_bucket_plugin.resolve(&connection, (*view.begin()).c_str(), buffer, &pathlen);

    if (!result) {
        return std::nullopt;
    }
    fs::path bucket_path(buffer, buffer + pathlen);
    // for(auto i = ++view.begin();i!=view.end();i++){
    //     bucket_path /= (*i).c_str();
    // }
    return bucket_path;
}

fs::path irods::s3::finish_path(const fs::path& base, const boost::urls::segments_view& view)
{
    auto result = base;
    for (auto i = ++view.begin(); i != view.end(); i++) {
        result /= (*i).c_str();
    }
    return result;
}
