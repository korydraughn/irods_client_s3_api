#include "irods/private/s3_api/s3_api.hpp"
#include "irods/private/s3_api/authentication.hpp"
#include "irods/private/s3_api/bucket.hpp"
#include "irods/private/s3_api/common_routines.hpp"
#include "irods/private/s3_api/connection.hpp"
#include "irods/private/s3_api/log.hpp"
#include "irods/private/s3_api/common.hpp"
#include "irods/private/s3_api/session.hpp"
#include "irods/private/s3_api/configuration.hpp"

#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>
#include <irods/irods_exception.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <fmt/format.h>
#include <regex>
#include <cstdio>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;
namespace log = irods::http::log;

static std::regex upload_id_pattern("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

void irods::s3::actions::handle_completemultipartupload(
    irods::http::session_pointer_type session_ptr,
    boost::beast::http::request_parser<boost::beast::http::empty_body>& empty_body_parser,
    const boost::urls::url_view& url)
{
    beast::http::response<beast::http::empty_body> response;

    // Authenticate
    auto irods_username = irods::s3::authentication::authenticates(empty_body_parser, url);
    if (!irods_username) {
        log::error("{}: Failed to authenticate.", __FUNCTION__);
        response.result(beast::http::status::forbidden);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    auto conn = irods::get_connection(*irods_username);
    
    std::filesystem::path s3_bucket;
    std::filesystem::path s3_key;

    bool on_bucket = true;
    for (auto seg : url.encoded_segments()) {
        if (on_bucket) {
            on_bucket = false;
            s3_bucket = seg.decode();
        } else {
            s3_key = s3_key / seg.decode();
        }
    }
    
    log::debug("{} s3_bucket={} s3_key={}", __FUNCTION__, s3_bucket.string(), s3_key.string());

    fs::path path;
    if (auto bucket = irods::s3::resolve_bucket(conn, url.segments()); bucket.has_value()) {
        path = bucket.value();
        path = irods::s3::finish_path(path, url.segments());
        log::debug("{}: CompleteMultipartUpload path={}", __FUNCTION__, path.string());
    } else {
        log::error("{}: Failed to resolve bucket", __FUNCTION__);
        response.result(beast::http::status::forbidden);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    fs::client::create_collections(conn, path.parent_path());

    // get the uploadId from the param list
    std::string upload_id;
    if (const auto upload_id_param = url.params().find("uploadId"); upload_id_param != url.params().end()) {
        upload_id = (*upload_id_param).value;
    }

    if (upload_id.empty()) {
        log::error("{}: Did not receive a an uploadId", __FUNCTION__);
        response.result(beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    // Do not allow an upload_id that is not in the format we have defined. People could do bad things
    // if we didn't enforce this.
    if (!std::regex_match(upload_id, upload_id_pattern)) {
        log::error("{}: Upload ID {} was not in expected format.", __FUNCTION__, upload_id);
        response.result(beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    beast::http::response<beast::http::string_body> string_body_response(std::move(response));
    string_body_response.result(beast::http::status::ok);

    // change the parser to a string_body parser and read the body
    empty_body_parser.eager(true);
    beast::http::request_parser<boost::beast::http::string_body> parser{std::move(empty_body_parser)};
    beast::http::read(session_ptr->stream().socket(), session_ptr->get_buffer(), parser);

    std::string& request_body = parser.get().body();
    log::debug("{}: request_body\n{}", __FUNCTION__, request_body);

    int max_part_number = -1;
    int min_part_number = 1000;
    int part_number_count = 0;
    boost::property_tree::ptree request_body_property_tree;
    try {
        std::stringstream ss;
        ss << request_body;
        boost::property_tree::read_xml(ss, request_body_property_tree);
        for (boost::property_tree::ptree::value_type& v : request_body_property_tree.get_child("CompleteMultipartUpload")) {
            const std::string& tag = v.first;
            if (tag == "Part") {
                for (boost::property_tree::ptree::value_type& v2 : v.second) {
                    const std::string& tag = v2.first;
                    if (tag == "PartNumber") {
                        int current_part_number = v2.second.get_value<int>(); 
                        if (current_part_number < min_part_number) {
                            min_part_number = current_part_number;
                        }
                        if (current_part_number > max_part_number) {
                            max_part_number = current_part_number;
                        }
                        ++part_number_count;
                    }
                }
            }
        }
    }
    catch (boost::property_tree::xml_parser_error &e) {
        log::debug("{}: Could not parse XML body.", __FUNCTION__);
        response.result(boost::beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }
    catch (...) {
        log::debug("{}: Unknown error parsing XML body.", __FUNCTION__);
        response.result(boost::beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    // At this point we are just checking that the part numbers start 
    // with 1 and the largest part number is the same as the count of
    // part numbers.  We could later check that each part number is included...
    if (min_part_number != 1) {
        log::debug("{}: Part numbers did not start with 1.", __FUNCTION__);
        response.result(boost::beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    if (max_part_number != part_number_count) {
        log::debug("{}: Missing at least one part number.", __FUNCTION__);
        response.result(boost::beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    // Very naive approach follows (for now)

    // get connection and transport
    irods::experimental::io::client::default_transport xtrans{conn};

    // read/write vector
    uint64_t read_buffer_size = irods::s3::get_put_object_buffer_size_in_bytes();
    std::vector<char> buf_vector(read_buffer_size);

    // open dstream to write to iRODS
    irods::experimental::io::odstream d;  // irods dstream for writing directly to irods
    d.open(xtrans, path, irods::experimental::io::root_resource_name{irods::s3::get_resource()}, std::ios_base::out);
    if (!d.is_open()) {
        log::error("{}: Failed to open dstream to iRODS - path={}", __FUNCTION__, path.string());
        response.result(beast::http::status::internal_server_error);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response));
        return;
    }

    // iterate through parts and write to iRODS
    for (int current_part_number = 1; current_part_number <= max_part_number; ++current_part_number) {

        // open stream for reading the current part
        std::string upload_part_filename;
        upload_part_filename = upload_id + "." + std::to_string(current_part_number);

        std::ifstream ifs;
        ifs.open(upload_part_filename, std::ifstream::in); 
        if (!ifs.is_open()) {
            log::error("{}: Failed to open stream for reading part", __FUNCTION__);
            response.result(beast::http::status::internal_server_error);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        }

        // read the file in parts into buffer and stream to iRODS
        while (ifs)
        {
            // Try to read next chunk of data
            ifs.read(buf_vector.data(), read_buffer_size);
            size_t read_bytes = ifs.gcount();
            if (!read_bytes) {
                break;
            }
            d.write((char*) buf_vector.data(), read_bytes);
        }
    }

    // delete the parts
    for (int current_part_number = 1; current_part_number <= max_part_number; ++current_part_number) {
        std::string upload_part_filename;
        upload_part_filename = upload_id + "." + std::to_string(current_part_number);
        std::remove(upload_part_filename.c_str());
    }

    // Now send the response
    // Example response:
    // <CompleteMultipartUploadResult>
    //     <Location>string</Location>
    //     <Bucket>string</Bucket>
    //     <Key>string</Key>
    //     <ETag>string</ETag>
    //  </CompleteMultipartUploadResult>

    boost::property_tree::ptree document;
    boost::property_tree::xml_parser::xml_writer_settings<std::string> settings;
    settings.indent_char = ' ';
    settings.indent_count = 4;
    std::stringstream s;

    std::string s3_region = irods::s3::get_s3_region();

    document.add("CompleteMultipartUploadResult.Location", s3_region);
    document.add("CompleteMultipartUploadResult.Bucket", s3_bucket.string());
    document.add("CompleteMultipartUploadResult.Key", s3_key);
    document.add("CompleteMultipartUploadResult.ETag", "TBD");

    boost::property_tree::write_xml(s, document, settings);
    string_body_response.body() = s.str();
    log::debug("{}: response\n{}", __FUNCTION__, s.str());
    string_body_response.result(boost::beast::http::status::ok);
    log::debug("{}: returned {}", __FUNCTION__, string_body_response.reason());
    session_ptr->send(std::move(string_body_response)); 
    return;
}
