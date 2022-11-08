#pragma once

#include <irods/rcConnect.h>
#include <string>
#include <boost/beast.hpp>
#include "types.hpp"
#include <boost/url.hpp>
namespace irods::s3::authentication
{
    bool authenticates(rcComm_t& conn, const static_buffer_request_parser& request, const boost::urls::url_view& url);
} //namespace irods::s3::authentication
