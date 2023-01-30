#include "s3_api.hpp"
#include "../authentication.hpp"
#include "../connection.hpp"
#include "../bucket.hpp"
#include "../types.hpp"
#include <boost/beast.hpp>
#include <boost/property_tree/ptree_fwd.hpp>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;

boost::asio::awaitable<void> irods::s3::actions::handle_copyobject(
    boost::asio::ip::tcp::socket& socket,
    static_buffer_request_parser& parser,
    const boost::urls::url_view& url)
{
    auto thing = irods::s3::get_connection();
    if (!irods::s3::authentication::authenticates(*thing, parser, url)) {
        beast::http::response<beast::http::empty_body> response;
        response.result(beast::http::status::forbidden);
        beast::http::write(socket, response);
        co_return;
    }
    auto url2 = boost::urls::url(parser.get()["x-amz-copy-source"]);
    fs::path destination_path, source_path;

    bool succeeds = false;
    if (auto bucket = irods::s3::resolve_bucket(*thing, url2.segments()); bucket.has_value()) {
        source_path = irods::s3::finish_path(bucket.value(), url2.segments());
        succeeds = true;
    }
    else {
        std::cerr << "Could not locate source path" << std::endl;
    }
    if (auto bucket = irods::s3::resolve_bucket(*thing, url.segments()); bucket.has_value()) {
        destination_path = irods::s3::finish_path(bucket.value(), url.segments());
        succeeds &= true;
    }
    if (!succeeds) {
        beast::http::response<beast::http::empty_body> response;
        response.result(beast::http::status::not_found);
        beast::http::write(socket, response);
        co_return;
    }
    fs::client::copy(*thing, source_path, destination_path);
    std::cerr << "Copied object!" << std::endl;
    // We don't have real etags, so the md5 here would be confusing, as it would match any number of distinct objects
    beast::http::response<beast::http::string_body> response;
    response.body() = "<CopyObjectResult></CopyObjectResult>";
    beast::http::write(socket, response);
    co_return;
}