#ifndef PERSISTENCE_HPP
#define PERSISTENCE_HPP
#include <optional>
#include <string_view>
#include <string>
#include <irods/filesystem.hpp>
#include "./multipart.hpp"

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
    // They also expect you to be able to do the bucket path reversal etc.

    /// Create a new multipart upload.
    /// \param connection The connection to the iRODS server
    /// \param path The path to create the upload at.
    /// \returns The id of the multipart upload, or std::nullopt
    std::optional<std::string> create_multipart_upload(rcComm_t* connection, const std::string_view& path);

    /// Finish a multipart upload, concatenating the files together.
    /// \param connection The connection to the iRODS server
    /// \param the final path of the multipart upload
    /// \param part_ids the list of part_ids specified to include in the finished file.
    /// \returns Whether or not the file was assembled as instructed.
    bool complete_multipart_upload(
        rcComm_t* connection,
        const std::string_view& path,
        const std::vector<std::string>& part_ids);

    /// Abort a multipart upload, deleting all partial uploads associated with it.
    /// \param connection The connection to the irods server.
    /// \param path The path of the upload.
    /// \returns If a multipart upload was successfully aborted.
    bool abort_multipart_upload(rcComm_t* connection, const std::string_view& path);

    /// Create a part of a multipart upload
    /// \param connection The connection to the iRODS server.
    /// \param upload_id The upload_id of the multipart upload
    /// \param part The part number of the multipart upload.
    /// \returns The path to the new part, or nullopt if it did not succeed.
    std::optional<std::string>
    create_part(rcComm_t* connection, const std::string_view& upload_id, const std::string_view& part);

    /// List the uploaded parts of the multipart upload.
    /// \param connection The connection to the irods server
    /// \param path The path of the multipart upload.
    /// \returns The part ids of the multipart upload
    std::vector<std::string> list_multipart_upload_parts(rcComm_t* connection, const std::string_view& path);

    struct multipart_result
    {
        std::string upload_id;
        std::string key;
    };
    /// List multipart uploads that are in progress(not completed or aborted).
    /// \param connection The connection to the iRODS server.
    /// \param path The path of the multipart upload in question.
    /// \returns A list of multipart uploads visible to the connection's user.x
    std::vector<std::string> list_multipart_uploads(
        rcComm_t* connection); // TODO This needs to actually return a multipart structure of some sort.
} //namespace irods::s3

#endif
