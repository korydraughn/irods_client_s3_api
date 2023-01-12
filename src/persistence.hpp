#ifndef PERSISTENCE_HPP
#define PERSISTENCE_HPP
#include <optional>
#include <string_view>
#include <string>
#include <irods/filesystem.hpp>

namespace irods::s3
{
    namespace persistence
    {
        /// Store a key value pair in the database.
        /// \param key The name of the key to store in the database.
        /// \param value The value to store.
        void store_key_value(const std::string_view& key, const std::string_view& value);
        /// Retrieve a value from the key value store.
        /// \param The name of the key to find the value of.
        /// \returns The value associated with the key, otherwise std::nullopt.
        std::optional<std::string> get_key_value(const std::string_view& key);
    } //namespace persistence

    // Multipart upload stuff.
    // Multipart upload functions expect that you resolve the path to the data object prior to
    // calling them.

    /// Create a new multipart upload.
    /// \param connection The connection to the iRODS server
    /// \param path The path to create the upload at.
    std::optional<std::string> create_multipart_upload(rcComm_t* connection, const std::string_view& path);

    /// Finish a multipart upload, concatenating the files together.
    /// \param connection The connection to the iRODS server
    /// \param the final path of the multipart upload
    /// \param part_ids the list of part_ids specified to include in the finished file.
    bool complete_multipart_upload(
        rcComm_t* connection,
        const std::string_view& path,
        const std::vector<std::string>& part_ids);

    /// Abort a multipart upload, deleting all partial uploads associated with it.
    /// \param connection The connection to the irods server.
    /// \param path The path of the upload.
    bool abort_multipart_upload(rcComm_t* connection, const std::string_view& path);

    std::vector<std::string> list_multipart_upload_parts(rcComm_t* connection, const std::string_view& path);

    std::vector<std::string> list_multipart_uploads(rcComm_t* connection, const std::string_view& path);
} //namespace irods::s3

#endif
