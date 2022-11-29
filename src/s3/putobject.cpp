#include "./s3_api.hpp"
#include "../connection.hpp"

#include "../authentication.hpp"
#include "../bucket.hpp"

#include <boost/beast/core/error.hpp>
#include <boost/beast/core/flat_static_buffer.hpp>
#include <boost/beast/core/static_buffer.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>

#include <iostream>

namespace asio = boost::asio;
namespace this_coro = boost::asio::this_coro;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;

boost::asio::awaitable<void> irods::s3::actions::handle_putobject(
    boost::asio::ip::tcp::socket& socket,
    static_buffer_request_parser& parser,
    const boost::urls::url_view& url)
{
    boost::beast::error_code ec;
    connection_handle connection = get_connection();
    std::cout << "Hi! About to auth" << std::endl;
    if (!irods::s3::authentication::authenticates(*connection, parser, url)) {
        boost::beast::http::response<boost::beast::http::empty_body> response;
        response.result(boost::beast::http::status::forbidden);
        boost::beast::http::write(socket, response);
        boost::beast::http::write(socket, response);
        std::cout << "Failed to auth" << std::endl;
        co_return;
    }

    if (parser.get()[boost::beast::http::field::expect] == "100-continue") {
        boost::beast::http::response<boost::beast::http::empty_body> resp;
        resp.version(11);
        resp.result(boost::beast::http::status::continue_);
        resp.set(boost::beast::http::field::server, parser.get()["Host"]);
        boost::beast::http::write(socket, resp, ec);
        std::cout << "Sent 100-continue" << std::endl;
        // {
        //     std::string str;

        //     auto buffer = boost::asio::dynamic_buffer(str);
        //     char buf[1000];
        //     parser.get().body().data = buf;
        //     parser.get().body().size = sizeof(buf);
        //     // boost::beast::flat_buffer buffer(1000);
        //     std::cout << "Abgout to read:" << std::endl;
        //     auto i = boost::beast::http::read(socket, buffer, parser, ec);
        //     std::cout << ec.what() << std::endl;
        //     std::cout << "First read after 100-continue produced [" << i << "] bytes\n";
        // }
    }

    fs::path path;
    if (auto bucket = irods::s3::resolve_bucket(*connection, url.segments()); bucket.has_value()) {
        path = std::move(bucket.value());
        path = irods::s3::finish_path(path, url.segments());
    }
    else {
        boost::beast::http::response<boost::beast::http::empty_body> response;
        response.result(boost::beast::http::status::not_found);
        std::cerr << "No bucket found" << std::endl;
        boost::beast::http::write(socket, response);
        co_return;
    }
    std::cout << "Path: [" << path << "]" << std::endl;
    // char buffer[4096];
    {
        boost::beast::flat_buffer buffer;
        boost::beast::http::response<boost::beast::http::empty_body> response;
        response.result(boost::beast::http::status::ok);

        irods::experimental::io::client::default_transport xtrans{*connection};
        irods::experimental::io::odstream d{xtrans, path};
        char buf[4096];
        parser.get().body().data = buf;
        parser.get().body().size = sizeof(buf);
        response.set("Etag", path.c_str());
        response.set("x-amz-id-2", "no");
        response.set("x-amz-request-id", "...");
        response.set("Connection", "close");
        while (!parser.is_done()) {
            auto read = boost::beast::http::read(socket, buffer, parser, ec);

            std::cout << "Read " << read << std::endl<<std::endl;
            std::cout.write((char*) parser.get().body().data, read);
            try {
                d.write((char*) parser.get().body().data, read);
            }
            catch (std::exception& e) {
                std::cout << e.what() << std::endl;
            }

            if (ec == beast::http::error::need_buffer) {
                ec = {};
            }
            else {
                co_return;
            }
            if (read == 0)
                break;
        }
        boost::beast::http::write(socket, response);
    }
    co_return;
}