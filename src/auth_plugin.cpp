#include "./auth_plugin.h"
#include "authentication.hpp"
#include "./plugin.hpp"

#include <optional>
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
    authentication_plugin active_authentication_plugin;
} //namespace

void add_authentication_plugin(
    secret_key_fn secret_key_function,
    get_iRODS_user_fn username_resolver,
    reset_user_fn reset_user_function,
    create_user_fn create_user_function,
    delete_user_fn delete_user_function,
    user_exists_fn user_exists)
{
    active_authentication_plugin = authentication_plugin{
        secret_key_function,
        username_resolver,
        reset_user_function,
        create_user_function,
        delete_user_function,
        user_exists};
}

bool irods::s3::plugins::authentication_plugin_loaded()
{
    return active_authentication_plugin.get_iRODS_user != nullptr;
}

bool irods::s3::authentication::delete_user(rcComm_t& connection, const std::string_view& username)
{
    return active_authentication_plugin.delete_user(&connection, username.data());
}

bool irods::s3::authentication::user_exists(rcComm_t& connection, const std::string_view& username)
{
    if (active_authentication_plugin.user_exists) {
        active_authentication_plugin.user_exists(&connection, username.data());
    }
    else {
        char irods_name[50];
        if (active_authentication_plugin.get_iRODS_user(&connection, username.data(), irods_name)) {
            return true;
        }
    }
    return false;
}

bool irods::s3::authentication::create_user(
    rcComm_t& connection,
    const std::string_view& username,
    const std::string_view& secret_key)
{
    if (user_exists(connection, username)) {
        return false;
    }
    if (active_authentication_plugin.create_user) {
        return active_authentication_plugin.create_user(
            &connection, username.data(), secret_key.data(), secret_key.length());
    }
    return false;
}
std::optional<std::string> irods::s3::authentication::get_iRODS_user(rcComm_t* conn, const std::string_view& user)
{
    char username[120];
    if (!active_authentication_plugin.get_iRODS_user(conn, user.data(), username)) {
        return std::nullopt;
    }
    return std::string(username);
}

std::optional<std::string> irods::s3::authentication::get_user_secret_key(rcComm_t* conn, const std::string_view& user)
{
    char secret_key[512];
    if (!active_authentication_plugin.secret_key(conn, user.data(), secret_key)) {
        return std::nullopt;
    }
    return std::string(secret_key);
}