#pragma once

#include <irods/rcConnect.h>
#include <string>
#include <string_view>
#include <boost/beast.hpp>
#include "types.hpp"
#include <boost/url.hpp>
#include <optional>

namespace irods::s3::authentication
{
    /// Authenticates an iRODS connection and switch to that user.
    /// \param conn The connection to the iRODS server.
    /// \param request The request.
    /// \param url The url
    /// \returns If the user was successfully authenticated.
    bool authenticates(rcComm_t& conn, const static_buffer_request_parser& request, const boost::urls::url_view& url);
    bool user_exists(rcComm_t& connection, const std::string_view& username);
    bool delete_user(rcComm_t& connection, const std::string_view& username);
    bool create_user(rcComm_t& connection, const std::string_view& username, const std::string_view& secret_key);
    bool user_exists(rcComm_t& connection, const std::string_view& username);
    
    std::optional<std::string> get_iRODS_user(rcComm_t* conn, const std::string_view& user);
    std::optional<std::string> get_user_secret_key(rcComm_t* conn, const std::string_view& user);
    
} //namespace irods::s3::authentication
