#ifndef IRODS_S3_API_S3_API_HPP
#define IRODS_S3_API_S3_API_HPP
#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/beast.hpp>
#include <irods/filesystem.hpp>
#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/url.hpp>

#include "irods/private/s3_api/types.hpp"
//#include "../connection.hpp"

namespace irods::s3::actions
{
    boost::asio::awaitable<void> handle_listobjects_v2(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);

    boost::asio::awaitable<void> handle_listbuckets(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);

    boost::asio::awaitable<void> handle_getobject(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);

    boost::asio::awaitable<void> handle_deleteobject(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);

    boost::asio::awaitable<void> handle_putobject(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);

    void handle_headobject(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url,
        boost::beast::http::response<boost::beast::http::string_body>& response);

    boost::asio::awaitable<void> handle_headbucket(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);

    boost::asio::awaitable<void> handle_copyobject(
        static_buffer_request_parser& parser,
        const boost::urls::url_view& url);
} //namespace irods::s3::actions
#endif
