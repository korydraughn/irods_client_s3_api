#include "./auth_plugin.h"
#include <vector>
#include <string>

namespace
{
    struct authentication_plugin
    {
        secret_key_fn secret_key;
        get_iRODS_user_fn get_iRODS_user;
        reset_user_fn reset_user;
        create_user_fn create_user;
        delete_user_fn delete_user;
        user_exists_fn user_exists;
    };
    std::vector<authentication_plugin> authentication_plugins;
} //namespace

extern void add_authentication_plugin(
    secret_key_fn secret_key_function,
    get_iRODS_user_fn username_resolver,
    reset_user_fn reset_user_function,
    create_user_fn create_user_function,
    delete_user_fn delete_user_function,
    user_exists_fn user_exists)
{
    authentication_plugins.emplace_back(authentication_plugin{
        secret_key_function,
        username_resolver,
        reset_user_function,
        create_user_function,
        delete_user_function,
        user_exists});
}

bool user_exists(rcComm_t& connection, const std::string_view& username)
{
    for (const auto& plugin : authentication_plugins) {
        if (plugin.user_exists) {
            if (plugin.user_exists(&connection, username.data()))
                return true;
        }
        else {
            char irods_name[50];
            if (plugin.get_iRODS_user(&connection, username.data(), irods_name)) {
                return true;
            }
        }
    }
    return false;
}
bool create_user(rcComm_t& connection, const std::string_view& username, const std::string_view& secret_key)
{
    // Check *every* authentication system for a matching user.
    if (user_exists(connection, username))
        return false;
    for (const auto& plugin : authentication_plugins) {
        if (plugin.create_user) {
            return plugin.create_user(&connection, username.data(), secret_key.data(), secret_key.length());
        }
    }
    return false;
}
