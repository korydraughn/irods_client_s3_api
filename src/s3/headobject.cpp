//
// Created by violet on 1/28/23.
//
#include "./s3_api.hpp"
#include "../authentication.hpp"
#include "../bucket.hpp"

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;

boost::asio::awaitable<void> irods::s3::actions::handle_headobject(
    boost::asio::ip::tcp::socket& socket,
    static_buffer_request_parser& parser,
    const boost::urls::url_view& url)
{
    auto thing = irods::s3::get_connection();
    beast::http::response<beast::http::empty_body> response;
    if (!irods::s3::authentication::authenticates(*thing, parser, url)) {
        response.result(beast::http::status::forbidden);
    }
    else {
        fs::path path;
        if (auto bucket = irods::s3::resolve_bucket(*thing, url.segments()); bucket.has_value()) {
            path = bucket.value();
            path = irods::s3::finish_path(path, url.segments());
        }
        boost::beast::http::response<boost::beast::http::empty_body> response;
        if (fs::client::exists(*thing, path)) {
            response.result(boost::beast::http::status::ok);
        }
        else {
            response.result(boost::beast::http::status::not_found);
        }
    }
    beast::http::write(socket, response);
    co_return;
}