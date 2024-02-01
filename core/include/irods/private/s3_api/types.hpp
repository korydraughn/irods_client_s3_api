#ifndef IRODS_S3_API_TYPES_HPP
#define IRODS_S3_API_TYPES_HPP
#include <boost/beast.hpp>

using static_buffer_request_parser = boost::beast::http::parser<true, boost::beast::http::string_body>;
#endif // IRODS_S3_API_TYPES_HPP
