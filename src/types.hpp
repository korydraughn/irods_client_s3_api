#pragma once
#include <boost/beast.hpp>

using static_buffer_request_parser = boost::beast::http::parser<true, boost::beast::http::buffer_body>;