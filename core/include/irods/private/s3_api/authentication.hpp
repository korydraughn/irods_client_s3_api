#ifndef IRODS_S3_API_AUTHENTICATION_HPP
#define IRODS_S3_API_AUTHENTICATION_HPP

#include "irods/private/s3_api/types.hpp"

#include <irods/rcConnect.h>
#include <string>
#include <string_view>
#include <boost/beast.hpp>
#include <boost/url.hpp>
#include <optional>

namespace irods::s3::authentication
{
    /// Resolves the hashed signature to an iRODS username.
    ///
    /// \param conn The connection to the iRODS server.
    /// \param request The request.
    /// \param url The url
    ///
    /// \returns An iRODS username if the signature is correct, else an empty std::optional.
    std::optional<std::string> authenticates(rcComm_t& conn, const static_buffer_request_parser& parser, const boost::urls::url_view& url);

    std::optional<std::string> get_iRODS_user(rcComm_t* conn, const std::string_view access_key);
    std::optional<std::string> get_user_secret_key(rcComm_t* conn, const std::string_view access_key);

} //namespace irods::s3::authentication
#endif
