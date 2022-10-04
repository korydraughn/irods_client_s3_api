#ifndef IRODS_S3_API_AUTH_PLUGIN_H
#define IRODS_S3_API_AUTH_PLUGIN_H
#include <stddef.h>
#include <irods/rcConnect.h>

#ifdef __cplusplus
extern "C" {
#endif

/// A function type for functions that provide the secret key for a given s3 username.
///
/// @param connection The irods server connection
/// @param username The username to get the secret key of.
/// @param access_key The output buffer for the secret key.
/// @returns true on being able to retrieve the secret key, false otherwise
typedef bool (*secret_key_fn)(rcComm_t* connection, const char* username, char* access_key);

/// A function type for mapping s3 users to iRODS users.
///
/// @param connection The connection to the irods server
/// @param username The username to look up
/// @param irods_username The output buffer to write to.
/// @returns true on successfully retrieving the mapping, false on failure.
typedef bool (*get_iRODS_user_fn)(rcComm_t* connection, const char* username, char* irods_username);

/// A function type for resetting the secret key of an S3 user.
///
/// @param connection the connection to the irods server
/// @param username The username to reset the secret key of.
/// @param key The secret key.
/// @param key_length The length of the secret key.
/// @returns true on success, false on failure.
typedef bool (*reset_user_fn)(rcComm_t* connection, const char* username, const char* key, size_t key_length);

/// A function type for deleting an s3 user.
///
/// @param connection the irods connection
/// @param username The user to delete
/// @returns true on successful deletion.
typedef bool (*delete_user_fn)(rcComm_t* connection, const char* username);

/// A function type for creating s3 users.
///
/// @param connection the irods connection
/// @param username The username of the s3 user to create
/// @param secret_key The secret key of the new user.
/// @param secret_key_length The length of the secret key.
/// \returns True when the user is successfully created, false otherwise
typedef bool (
    *create_user_fn)(rcComm_t* connection, const char* username, const char* secret_key, size_t secret_key_length);

/// A function type for checking if a user exists.
///
/// @param connection The connection to the iRODS server
/// @param username The username to check for;
/// @returns true when the user exists.
typedef bool (*user_exists_fn)(rcComm_t* connection, const char* username);

#ifdef BRIDGE_PLUGIN
extern
#endif // BRIDGE_PLUGIN
    /// Add an authentication plugin to the back of the authentication plugin list.
    /// @param secret_key_function A function to resolve the secret key. Required to not be nullptr
    /// @param username_resolver A function to resolve the irods username from an s3 username. Required to not be a
    /// nullptr
    /// @param reset_user_function A function to reset a user's secret key, optional
    /// @param create_user_function A function to create a user, optional.
    /// @param delete_user_function A function to delete a user, optional
    /// @param user_exists_fn a function to check if a user exists, optional
    void
    add_authentication_plugin(
        secret_key_fn secret_key_function,
        get_iRODS_user_fn username_resolver,
        reset_user_fn reset_user_function,
        create_user_fn create_user_function,
        delete_user_fn delete_user_function,
        user_exists_fn user_exists_fn);

#ifdef __cplusplus
}
#endif // __cplusplus
#endif // IRODS_S3_API_AUTH_PLUGIN_H
