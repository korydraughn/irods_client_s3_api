#pragma once

#include "boost/url.hpp"
#include <irods/filesystem.hpp>

namespace irods::s3
{
    /// Get the base path of the given bucket in the request.
    /// 
    /// \param connection The connection to the irods server.
    /// \param view The view of the url's segments.
    irods::experimental::filesystem::path resolve_bucket(rcComm_t& connection, const boost::urls::segments_view&);

    /// Get the final path of the given bucket in the request.
    /// 
    /// \param base The base irods path for the bucket
    /// \param view The view of the url's segments.
    irods::experimental::filesystem::path finish_path(
        const irods::experimental::filesystem::path& base,
        const boost::urls::segments_view&);
    std::string strip_bucket(const std::string&);
} //namespace irods::s3
