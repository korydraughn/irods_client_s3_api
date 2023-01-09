#include "../types.hpp"
#include "s3_api.hpp"
#include "../bucket.hpp"
#include "../connection.hpp"
#include "../authentication.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <experimental/coroutine>
#include <boost/beast.hpp>

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/url.hpp>

#include <irods/filesystem.hpp>
#include <irods/query_builder.hpp>

#include <iostream>
#include <unordered_set>

#include <fmt/format.h>

namespace asio = boost::asio;
namespace beast = boost::beast;

asio::awaitable<void> irods::s3::actions::handle_listobjects_v2(
    asio::ip::tcp::socket& socket,
    static_buffer_request_parser& parser,
    const boost::urls::url_view& url)
{
    using namespace boost::property_tree;

    auto thing = irods::s3::get_connection();

    if (!irods::s3::authentication::authenticates(*thing, parser, url)) {
        beast::http::response<beast::http::empty_body> response;
        response.result(beast::http::status::forbidden);
        beast::http::write(socket, response);
        co_return;
    }

    irods::experimental::filesystem::path resolved_path;
    if (auto bucket = irods::s3::resolve_bucket(*thing, url.segments()); bucket.has_value()) {
        resolved_path = bucket.value();
    }
    else {
        beast::http::response<beast::http::empty_body> response;
        response.result(beast::http::status::not_found);
        beast::http::write(socket, response);
        co_return;
    }
    auto base_length = resolved_path.string().size();
    resolved_path = irods::s3::finish_path(resolved_path, url.segments());
    boost::property_tree::ptree document;

    std::string filename_prefix = "%";

    if (const auto prefix = url.params().find("prefix"); prefix != url.params().end()) {
        filename_prefix = (*prefix).value + "%";
    }

    std::string query = fmt::format(
        "select COLL_NAME,DATA_NAME,DATA_OWNER_NAME,DATA_SIZE where COLL_NAME like '{}%' AND DATA_NAME like "
        "'{}'",
        resolved_path.c_str(),
        filename_prefix,
        resolved_path.string().substr(0, resolved_path.string().length() - 1),
        filename_prefix);

    std::cout << query << std::endl;

    auto contents = document.add("ListBucketResult", "");

    bool found_objects = false;
    std::unordered_set<std::string> seen_keys;

    for (auto&& i : irods::query<RcComm>(thing.get(), query)) {
        found_objects = true;
        ptree object;
        object.put("Key", i[1].substr(base_length));
        object.put("Etag", i[0]);
        object.put("Owner", i[2]);
        object.put("Size", atoi(i[3].c_str()));
        // add_child always creates a new node, put_child would replace the previous one.
        document.add_child("ListBucketResult.Contents", object);
    }

    // Required for genquery limitations :p
    query = fmt::format(
        "select COLL_NAME,DATA_NAME,DATA_OWNER_NAME,DATA_SIZE where COLL_NAME like '{}/{}'",
        resolved_path.string().substr(0, resolved_path.string().length() - 1),
        filename_prefix);
    std::cout << query << std::endl;
    for (auto&& i : irods::query<RcComm>(thing.get(), query)) {
        found_objects = true;
        ptree object;
        object.put("Key", i[0].substr(base_length) + "/" + i[1]);
        object.put("Etag", i[0] + "/" + i[1]);
        object.put("Owner", i[2]);
        object.put("Size", atoi(i[3].c_str()));
        document.add_child("ListBucketResult.Contents", object);
    }

    if (found_objects) {
        std::stringstream s;
        boost::property_tree::xml_parser::xml_writer_settings<std::string> settings;
        settings.indent_char = ' ';
        settings.indent_count = 4;
        std::cout << "Found objects" << std::endl;
        boost::property_tree::write_xml(s, document, settings);
        beast::http::response<beast::http::string_body> response;
        response.body() = s.str();
        std::cout << s.str();

        beast::http::write(socket, response);
    }
    else {
        std::cout << "Couldn't find anything" << std::endl;
        beast::http::response<beast::http::empty_body> response;
        response.result(beast::http::status::not_found);
        beast::http::write(socket, response);
    }
    co_return;
}