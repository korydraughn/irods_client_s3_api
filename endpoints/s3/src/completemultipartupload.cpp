#include "irods/private/s3_api/s3_api.hpp"
#include "irods/private/s3_api/authentication.hpp"
#include "irods/private/s3_api/bucket.hpp"
#include "irods/private/s3_api/common_routines.hpp"
#include "irods/private/s3_api/connection.hpp"
#include "irods/private/s3_api/log.hpp"
#include "irods/private/s3_api/common.hpp"
#include "irods/private/s3_api/session.hpp"
#include "irods/private/s3_api/configuration.hpp"

#include <irods/irods_exception.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <fmt/format.h>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;
namespace log = irods::http::log;

void irods::s3::actions::handle_completemultipartupload(
    irods::http::session_pointer_type session_ptr,
    static_buffer_request_parser& parser,
    const boost::urls::url_view& url)
{
    beast::http::response<beast::http::empty_body> response;

    // Authenticate
    auto irods_username = irods::s3::authentication::authenticates(parser, url);
    if (!irods_username) {
        log::error("{}: Failed to authenticate.", __FUNCTION__);
        response.result(beast::http::status::forbidden);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    auto conn = irods::s3::get_connection(*irods_username);
    
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
    if (auto bucket = irods::s3::resolve_bucket(*conn, url.segments()); bucket.has_value()) {
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

    fs::client::create_collections(*conn, path.parent_path());

    // get the uploadId from the param list
    std::string upload_id;
    if (const auto upload_id_param = url.params().find("uploadId"); upload_id_param != url.params().end()) {
        upload_id = (*upload_id_param).value;
    }

    if (upload_id.empty()) {
        log::error("{}: Did not receive an uploadId", __FUNCTION__);
        response.result(beast::http::status::bad_request);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    beast::http::response<beast::http::string_body> string_body_response(std::move(response));
    string_body_response.result(beast::http::status::ok);

    std::string& request_body = parser.get().body();
    log::debug("{}: request_body\n{}", __FUNCTION__, request_body);

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
