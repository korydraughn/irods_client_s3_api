#include "s3_api.hpp"

#include "../connection.hpp"
#include "../bucket.hpp"
#include "../authentication.hpp"

#include <irods/filesystem.hpp>

#include <irods/filesystem/collection_entry.hpp>
#include <irods/filesystem/collection_iterator.hpp>
#include <irods/filesystem/filesystem.hpp>
#include <irods/filesystem/filesystem_error.hpp>
#include <irods/filesystem/path.hpp>
#include <irods/genQuery.h>
#include <irods/irods_exception.hpp>
#include <irods/msParam.h>
#include <irods/rcConnect.h>
#include <irods/rodsClient.h>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>
#include <irods/irods_parse_command_line_options.hpp>
#include <irods/filesystem.hpp>
#include <irods/rcMisc.h>
#include <irods/rodsGenQuery.h>
#include <irods/rodsPath.h>
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
#include <irods/irods_query.hpp>
#include <irods/query_builder.hpp>
#include <irods/rodsErrorTable.h>

#include <boost/stacktrace.hpp>

namespace asio = boost::asio;
namespace this_coro = boost::asio::this_coro;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;

asio::awaitable<void> irods::s3::actions::handle_getobject(
    asio::ip::tcp::socket& socket,
    static_buffer_request_parser& parser,
    const boost::urls::url_view& url)
{
    auto thing = irods::s3::get_connection();
    auto url_and_stuff = boost::urls::url_view(parser.get().base().target());
    // Permission verification stuff should go roughly here.
    if (!irods::s3::authentication::authenticates(*thing, parser, url)) {
        boost::beast::http::response<boost::beast::http::empty_body> response;
        response.result(boost::beast::http::status::forbidden);
        boost::beast::http::write(socket, response);
        co_return;
    }
    fs::path path;
    if (auto bucket = irods::s3::resolve_bucket(*thing, url.segments()); bucket.has_value()) {
        path = bucket.value();
        path = irods::s3::finish_path(path, url.segments());
    }
    else {
        boost::beast::http::response<boost::beast::http::empty_body> response;
        response.result(boost::beast::http::status::not_found);
        std::cerr << "Could not find bucket" << std::endl;
        boost::beast::http::write(socket, response);

        co_return;
    }
    std::cout << "Requested " << path << std::endl;

    try {
        if (fs::client::exists(*thing, path)) {
            std::cout << "Trying to write file" << std::endl;
            boost::beast::http::response<boost::beast::http::buffer_body> response;
            boost::beast::http::response_serializer<boost::beast::http::buffer_body> serializer{response};
            char buffer_backing[4096];

            std::string length_field =
                std::to_string(irods::experimental::filesystem::client::data_object_size(*thing, path));
            response.insert(boost::beast::http::field::content_length, length_field);
            auto md5 = irods::experimental::filesystem::client::data_object_checksum(*thing, path);
            response.insert("Content-MD5", md5);

            boost::beast::error_code ec;
            irods::experimental::io::client::default_transport xtrans{*thing};
            irods::experimental::io::idstream d{xtrans, path};

            if (d.fail() || d.bad()) {
                std::cout << "Fail/badbit set" << std::endl;
                response.result(boost::beast::http::status::forbidden);
                response.body().more = false;
                boost::beast::http::write(socket, response);
                co_return;
            }
            boost::beast::http::write_header(socket, serializer);
            std::streampos current, size;
            while (d.good()) {
                response.result(boost::beast::http::status::ok);
                d.read(buffer_backing, 4096);
                current = d.gcount();
                size += current;
                response.body().data = buffer_backing;
                response.body().size = current;
                std::cout << "Wrote " << current << " bytes" << std::endl;
                if (d.bad()) {
                    std::cerr << "Weird error?" << std::endl;
                    exit(12);
                }
                try {
                    boost::beast::http::write(socket, serializer);
                }
                catch (boost::system::system_error& e) {
                    if (e.code() != boost::beast::http::error::need_buffer) {
                        std::cout << "Not a good error!" << std::endl;
                        throw e;
                    }
                    else {
                        // It would be nice if we could figure out something a bit more
                        // semantic than catching an exception
                        std::cout << "Good error!" << std::endl;
                    }
                }
            }
            response.body().size = d.gcount();
            response.body().more = false;
            boost::beast::http::write(socket, serializer);
            std::cout << "Wrote " << size << " bytes total" << std::endl;
        }
        else {
            boost::beast::http::response<boost::beast::http::empty_body> response;
            response.result(boost::beast::http::status::not_found);
            std::cerr << "Could not find file" << std::endl;
            boost::beast::http::write(socket, response);
        }
    }
    catch (irods::exception& e) {
        boost::beast::http::response<boost::beast::http::empty_body> response;
        std::cout << "Exception! in the getobject" << std::endl;
        response.result(boost::beast::http::status::forbidden);

        switch (e.code()) {
            case USER_ACCESS_DENIED:
            case CAT_NO_ACCESS_PERMISSION:
                response.result(boost::beast::http::status::forbidden);
                break;
            default:
                response.result(boost::beast::http::status::internal_server_error);
                break;
        }

        boost::beast::http::write(socket, response);
        co_return;
    }
    catch (std::exception& e) {
        std::cout << boost::stacktrace::stacktrace() << std::endl;
        std::cout << "error! " << e.what() << std::endl;
    }

    boost::beast::http::response<boost::beast::http::dynamic_body> response;

    co_return;
}