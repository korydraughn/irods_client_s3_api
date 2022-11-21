#pragma once

#include <irods/rcConnect.h>
#include <string>
#include <boost/beast.hpp>
#include "types.hpp"
#include <boost/url.hpp>
#include <optional>

namespace irods::s3::authentication
{
    bool authenticates(rcComm_t& conn, const static_buffer_request_parser& request, const boost::urls::url_view& url);
    bool user_exists(rcComm_t& connection, const std::string_view& username);
    bool delete_user(rcComm_t& connection, const std::string_view& username);
    bool create_user(rcComm_t& connection, const std::string_view& username, const std::string_view& secret_key);
    bool user_exists(rcComm_t& connection, const std::string_view& username);
    std::optional<std::string> get_user_secret_key(rcComm_t* conn, const std::string_view& user);
    
} //namespace irods::s3::authentication
