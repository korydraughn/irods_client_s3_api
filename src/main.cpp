#include "./authentication.hpp"
#include "./event_loop.hpp"
#include "./hmac.hpp"
#include "./bucket.hpp"
#include "boost/url/param.hpp"
#include "boost/url/url.hpp"
#include "boost/url/url_view.hpp"
#include "./connection.hpp"
#include "./types.hpp"
#include "plugin.hpp"
#include "s3/s3_api.hpp"

#include <algorithm>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/detail/descriptor_ops.hpp>
#include <boost/asio/detail/socket_ops.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/detail/type_traits.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/error.hpp>
#include <boost/beast/http/fields.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/property_tree/detail/xml_parser_writer_settings.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <boost/stacktrace.hpp>

#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/type_traits.hpp>

#include <boost/beast/http/verb.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/stacktrace/stacktrace_fwd.hpp>
#include <boost/system/system_error.hpp>
#include <boost/url/src.hpp>

#include <chrono>
#include <ctype.h>
#include <experimental/coroutine>
#include <filesystem>
#include <iomanip>
#include <ios>
#include <iostream>
#include <fstream>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <irods/filesystem/collection_entry.hpp>
#include <irods/filesystem/collection_iterator.hpp>
#include <irods/filesystem/filesystem.hpp>
#include <irods/filesystem/filesystem_error.hpp>
#include <irods/filesystem/path.hpp>
#include <irods/genQuery.h>
#include <irods/msParam.h>
#include <irods/rcConnect.h>
#include <irods/rodsClient.h>
#include <irods/client_connection.hpp>
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

#include <memory>
#include <string_view>
#include <type_traits>
#include <unordered_set>

namespace asio = boost::asio;
namespace this_coro = boost::asio::this_coro;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;

// asio::awaitable<void> handle_listobjects_v2(
//     asio::ip::tcp::socket& socket,
//     static_buffer_request_parser& parser,
//     const boost::urls::url_view& url)
// {
//     using namespace boost::property_tree;

//     auto thing = irods::s3::get_connection();

//     if (!irods::s3::authentication::authenticates(*thing, parser, url)) {
//         boost::beast::http::response<boost::beast::http::empty_body> response;
//         response.result(boost::beast::http::status::forbidden);
//         boost::beast::http::write(socket, response);
//         co_return;
//     }

//     irods::experimental::filesystem::path resolved_path;
//     if (auto bucket = irods::s3::resolve_bucket(*thing, url.segments()); bucket.has_value()) {
//         resolved_path = bucket.value();
//     }
//     else {
//         boost::beast::http::response<boost::beast::http::empty_body> response;
//         response.result(boost::beast::http::status::not_found);
//         boost::beast::http::write(socket, response);
//         co_return;
//     }
//     auto base_length = resolved_path.string().size();
//     resolved_path = irods::s3::finish_path(resolved_path, url.segments());
//     boost::property_tree::ptree document;

//     std::string filename_prefix = "%";

//     if (const auto prefix = url.params().find("prefix"); prefix != url.params().end()) {
//         filename_prefix = (*prefix).value + "%";
//     }

//     std::string query = fmt::format(
//         "select COLL_NAME,DATA_NAME,DATA_OWNER_NAME,DATA_SIZE where COLL_NAME like '{}%' AND DATA_NAME like "
//         "'{}'",
//         resolved_path.c_str(),
//         filename_prefix,
//         resolved_path.string().substr(0, resolved_path.string().length() - 1),
//         filename_prefix);

//     std::cout << query << std::endl;

//     auto contents = document.add("ListBucketResult", "");

//     bool found_objects = false;
//     std::unordered_set<std::string> seen_keys;

//     for (auto&& i : irods::query<RcComm>(thing.get(), query)) {
//         found_objects = true;
//         ptree object;
//         object.put("Key", i[1].substr(base_length));
//         object.put("Etag", i[1]);
//         object.put("Owner", i[2]);
//         object.put("Size", atoi(i[3].c_str()));
//         // add_child always creates a new node, put_child would replace the previous one.
//         document.add_child("ListBucketResult.Contents", object);
//     }

//     // Required for genquery limitations :p
//     query = fmt::format(
//         "select COLL_NAME,DATA_NAME,DATA_OWNER_NAME,DATA_SIZE where COLL_NAME like '{}/{}'",
//         resolved_path.string().substr(0, resolved_path.string().length() - 1),
//         filename_prefix);
//     std::cout << query << std::endl;
//     for (auto&& i : irods::query<RcComm>(thing.get(), query)) {
//         found_objects = true;
//         ptree object;
//         object.put("Key", i[0].substr(base_length) + "/" + i[1]);
//         object.put("Etag", i[1]);
//         object.put("Owner", i[2]);
//         object.put("Size", atoi(i[3].c_str()));
//         document.add_child("ListBucketResult.Contents", object);
//     }

//     if (found_objects) {
//         std::stringstream s;
//         boost::property_tree::xml_parser::xml_writer_settings<std::string> settings;
//         settings.indent_char = ' ';
//         settings.indent_count = 4;
//         std::cout << "Found objects" << std::endl;
//         boost::property_tree::write_xml(s, document, settings);
//         boost::beast::http::response<boost::beast::http::string_body> response;
//         response.body() = s.str();
//         std::cout << s.str();

//         boost::beast::http::write(socket, response);
//     }
//     else {
//         std::cout << "Couldn't find anything" << std::endl;
//         boost::beast::http::response<boost::beast::http::empty_body> response;
//         response.result(boost::beast::http::status::not_found);
//         boost::beast::http::write(socket, response);
//     }
//     co_return;
// }

// asio::awaitable<void>
// handle_getobject(asio::ip::tcp::socket& socket, static_buffer_request_parser& parser, const boost::urls::url_view& url)
// {
//     auto thing = irods::s3::get_connection();
//     auto url_and_stuff = boost::urls::url_view(parser.get().base().target());
//     // Permission verification stuff should go roughly here.

//     fs::path path;
//     if (auto bucket = irods::s3::resolve_bucket(*thing, url.segments()); bucket.has_value()) {
//         path = bucket.value();
//         path = irods::s3::finish_path(path, url.segments());
//     }
//     else {
//         boost::beast::http::response<boost::beast::http::empty_body> response;
//         response.result(boost::beast::http::status::not_found);
//         std::cerr << "Could not find file" << std::endl;
//         boost::beast::http::write(socket, response);

//         co_return;
//     }
//     std::cout << "Requested " << path << std::endl;

//     try {
//         if (fs::client::exists(*thing, path)) {
//             std::cout << "Trying to write file" << std::endl;
//             boost::beast::http::response<boost::beast::http::buffer_body> response;
//             boost::beast::http::response_serializer<boost::beast::http::buffer_body> serializer{response};
//             char buffer_backing[4096];
//             response.result(boost::beast::http::status::accepted);
//             std::string length_field =
//                 std::to_string(irods::experimental::filesystem::client::data_object_size(*thing, path));
//             response.insert(boost::beast::http::field::content_length, length_field);
//             auto md5 = irods::experimental::filesystem::client::data_object_checksum(*thing, path);
//             response.insert("Content-MD5", md5);
//             boost::beast::http::write_header(socket, serializer);
//             boost::beast::error_code ec;
//             irods::experimental::io::client::default_transport xtrans{*thing};
//             irods::experimental::io::idstream d{xtrans, path};

//             std::streampos current, size;
//             while (d.good()) {
//                 d.read(buffer_backing, 4096);
//                 current = d.gcount();
//                 size += current;
//                 response.body().data = buffer_backing;
//                 response.body().size = current;
//                 std::cout << "Wrote " << current << " bytes" << std::endl;
//                 if (d.bad()) {
//                     std::cerr << "Weird error?" << std::endl;
//                     exit(12);
//                 }
//                 try {
//                     boost::beast::http::write(socket, serializer);
//                 }
//                 catch (boost::system::system_error& e) {
//                     if (e.code() != boost::beast::http::error::need_buffer) {
//                         std::cout << "Not a good error!" << std::endl;
//                         throw e;
//                     }
//                     else {
//                         // It would be nice if we could figure out something a bit more
//                         // semantic than catching an exception
//                         std::cout << "Good error!" << std::endl;
//                     }
//                 }
//             }
//             response.body().size = d.gcount();
//             response.body().more = false;
//             boost::beast::http::write(socket, serializer);
//             std::cout << "Wrote " << size << " bytes total" << std::endl;
//         }
//         else {
//             boost::beast::http::response<boost::beast::http::empty_body> response;
//             response.result(boost::beast::http::status::not_found);
//             std::cerr << "Could not find file" << std::endl;
//             boost::beast::http::write(socket, response);
//         }
//     }
//     catch (std::exception& e) {
//         std::cout << boost::stacktrace::stacktrace() << std::endl;
//         std::cout << "error! " << e.what() << std::endl;
//     }

//     boost::beast::http::response<boost::beast::http::dynamic_body> response;

//     co_return;
// }

// for now let's just list out what we get.
asio::awaitable<void> handle_request(asio::ip::tcp::socket socket)
{
    beast::http::parser<true, beast::http::buffer_body> parser;

    std::string buf_back;
    auto buffer = beast::flat_buffer();
    boost::system::error_code ec;
    std::cout << "Received " << beast::http::read_header(socket, buffer, parser, ec) << " bytes\n";
    if (ec) {
        // handle me please!
    }
    for (const auto& field : parser.get()) {
        std::cout << "header: " << field.name_string() << ":" << field.value() << std::endl;
    }
    std::cout << "target: " << parser.get().target() << std::endl;
    std::string url_string = parser.get().find("Host")->value().to_string() + parser.get().target().to_string();
    std::cout << "candidate url string is [" << url_string << "]\n";
    boost::urls::url url2;
    auto host = parser.get().find("Host")->value();
    url2.set_encoded_host(host.find(':') != std::string::npos ? host.substr(0, host.find(':')) : host);
    url2.set_path(parser.get().target().substr(0, parser.get().target().find("?")));
    url2.set_scheme("http");
    if (parser.get().target().find('?') != std::string::npos) {
        url2.set_query(parser.get().target().substr(parser.get().target().find("?") + 1));
    }

    boost::urls::url_view url = url2;
    std::cout << "Url " << url2 << std::endl;
    const auto& segments = url.segments();
    const auto& params = url.params();
    std::cout << segments << " " << segments.empty() << std::endl;
    switch (parser.get().method()) {
        case boost::beast::http::verb::get:
            if (segments.empty() || params.contains("encoding-type") || params.contains("list-type")) {
                // Among other things, listobjects should be handled here.

                // This is a weird little thing because the parameters are a multimap.
                auto f = url.params().find("list-type");

                // Honestly not being able to use -> here strikes me as a potential mistake that
                // will be corrected in the future when boost::url is released properly as part
                // of boost
                if (f != url.params().end() && (*f).value == "2") {
                    co_await irods::s3::actions::handle_listobjects_v2(socket, parser, url);
                }
            }
            else {
                // GetObject
                std::cout << "getobject detected" << std::endl;
                co_await irods::s3::actions::handle_getobject(socket, parser, url);
            }
            break;
        case boost::beast::http::verb::put:
            if (parser.get().find("x-amz-copy-source") != parser.get().end()) {
                // copyobject
                std::cout << "Copyobject detected" << std::endl;
            }
            else {
                // putobject
                std::cout << "putobject detected" << std::endl;
            }
            break;
        case boost::beast::http::verb::head:
            // Probably just headbucket and headobject here.
            // and headbucket isn't on the immediate list
            // of starting point.
            if (url.segments().empty()) {
                std::cout << "Headbucket detected" << std::endl;
            }
            else {
                std::cout << "Headobject detected" << std::endl;
            }
            break;
        case boost::beast::http::verb::delete_:
            // DeleteObject
            std::cout << "Deleteobject detected" << std::endl;
            break;
        default:
            std::cerr << "Oh no..." << std::endl;
            exit(37);
            break;
    }
    char buf[512];
    while (!parser.is_done()) {
        std::cout << "Reading" << std::endl;
        parser.get().body().data = buf;
        parser.get().body().size = sizeof(buf);
        // Using async_read here causes it to completely eat the entire input without handling it properly.
        auto read = co_await beast::http::async_read_some(socket, buffer, parser, asio::use_awaitable);
        std::cout << "Read " << read << " bytes" << std::endl;
        if (ec == beast::http::error::need_buffer) {
            ec = {};
        }
        else if (ec) {
            co_return;
        }
    }
    co_return;
}

asio::awaitable<void> listener()
{
    auto executor = co_await this_coro::executor;
    asio::ip::tcp::acceptor acceptor(executor, {asio::ip::tcp::v4(), 8080});
    for (;;) {
        asio::ip::tcp::socket socket = co_await acceptor.async_accept(boost::asio::use_awaitable);
        asio::co_spawn(executor, handle_request(std::move(socket)), asio::detached);
        std::cout << "Accepted?" << std::endl;
    }
}
int main()
{
    using namespace nlohmann::literals;
    irods::api_entry_table& api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl = irods::get_pack_table();
    init_api_table(api_tbl, pk_tbl);
    {
        auto config = R"({"name":"static_bucket_resolver", "mappings": {"wow": "/tempZone/home/rods/wow/"}})"_json;
        auto i = irods::s3::get_connection();
        irods::s3::plugins::load_plugin(*i, "static_bucket_resolver", config);
    }
    {
        auto config =
            R"({"name":"static_authentication_resolver", "users": {"no": {"username":"rods","secret_key":"heck"}}})"_json;
        auto i = irods::s3::get_connection();
        irods::s3::plugins::load_plugin(*i, "static_authentication_resolver", config);
    }

    asio::io_context io_context(1);
    auto address = asio::ip::make_address("0.0.0.0");
    asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto) { io_context.stop(); });
    asio::co_spawn(io_context, listener(), boost::asio::detached);
    io_context.run();
    return 0;
}
