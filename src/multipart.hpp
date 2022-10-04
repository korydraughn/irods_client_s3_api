//
// Created by violet on 1/25/23.
//

#ifndef IRODS_S3_BRIDGE_MULTIPART_HPP
#define IRODS_S3_BRIDGE_MULTIPART_HPP
#include <string_view>
#include <string>

namespace irods::s3::multipart
{
    namespace utilities
    {
        const std::string encode_path(const std::string_view& sv);
        /// Get the path for a temporary file for the upload
        /// \param path The eventual path that the multipart upload will be concatenated into
        /// \param part_number The number of the part.
        const std::string get_temporary_file(const std::string_view& path, const std::string_view& part_number);
    } //namespace utilities
} //namespace irods::s3::multipart
#endif //IRODS_S3_BRIDGE_MULTIPART_HPP
