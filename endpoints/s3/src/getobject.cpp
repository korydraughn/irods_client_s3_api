#include "irods/private/s3_api/s3_api.hpp"
#include "irods/private/s3_api/authentication.hpp"
#include "irods/private/s3_api/bucket.hpp"
#include "irods/private/s3_api/common_routines.hpp"
#include "irods/private/s3_api/connection.hpp"
#include "irods/private/s3_api/log.hpp"
#include "irods/private/s3_api/common.hpp"
#include "irods/private/s3_api/session.hpp"
#include "irods/private/s3_api/configuration.hpp"

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
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;
namespace log = irods::http::log;

const static std::string_view date_format{"{:%a, %d %b %Y %H:%M:%S GMT}"};

void irods::s3::actions::handle_getobject(
    irods::http::session_pointer_type session_ptr,
    beast::http::request_parser<boost::beast::http::empty_body>& parser,
    const boost::urls::url_view& url)
{
    beast::http::response<beast::http::empty_body> response;

    // Permission verification stuff should go roughly here.

    auto irods_username = irods::s3::authentication::authenticates(parser, url);
    if (!irods_username) {
        log::error("{}: Failed to authenticate.", __FUNCTION__);
        response.result(beast::http::status::forbidden);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    auto conn = irods::get_connection(*irods_username); 

    fs::path path;
    if (auto bucket = irods::s3::resolve_bucket(conn, url.segments()); bucket.has_value()) {
        path = bucket.value();
        path = irods::s3::finish_path(path, url.segments());
    }
    else {
        log::error("{}: Failed to resolve bucket", __FUNCTION__);
        response.result(beast::http::status::not_found);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    // read the range header if it exists
    // Note:  We are only implementing range headers in the format range: bytes=<start>-[end]
    std::size_t range_start = 0;
    std::size_t range_end = 0;
    auto range_header = parser.get().find("range");
    if (range_header != parser.get().end()) {
        if (range_header->value().starts_with("bytes=")) {
            std::string range = range_header->value().substr(6);

            std::vector<std::string> range_parts;
            boost::split(range_parts, range, boost::is_any_of("-"));

            if (range_parts.size() != 2) {
                log::error("{}: The provided range format has not been implemented.", __FUNCTION__);
                response.result(beast::http::status::not_implemented);
                log::debug("{}: returned {}", __FUNCTION__, response.reason());
                session_ptr->send(std::move(response)); 
                return;
            }

            try {
                range_start = boost::lexical_cast<std::size_t>(range_parts[0]);
                if (!range_parts[1].empty()) {
                    range_end = boost::lexical_cast<std::size_t>(range_parts[1]);
                }
            }
            catch (const boost::bad_lexical_cast&) {
                log::error("{}: Could not cast the start or end range to a size_t.", __FUNCTION__);
                response.result(beast::http::status::not_implemented);
                log::debug("{}: returned {}", __FUNCTION__, response.reason());
                session_ptr->send(std::move(response)); 
                return;
            }

        }
        else {
            log::error("{}: The provided range format has not been implemented - does not begin with \"bytes=\".", __FUNCTION__);
            response.result(beast::http::status::not_implemented);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response)); 
            return;
        }
    } 

    try {
        if (fs::client::exists(conn, path)) {
            beast::http::response<beast::http::buffer_body> buffer_body_response{std::move(response)};
            beast::http::response_serializer<beast::http::buffer_body> serializer{buffer_body_response};

            uint64_t write_buffer_size = irods::s3::get_get_object_buffer_size_in_bytes();
            log::debug("{}: Write buffer size = {}", __FUNCTION__, write_buffer_size);
            std::vector<char> buf_vector(write_buffer_size);

            auto file_size = irods::experimental::filesystem::client::data_object_size(conn, path);
            if (range_end == 0 || range_end > file_size - 1) {
                range_end = file_size - 1;
            }
            auto content_length = range_end - range_start + 1;  // ranges are inclusive

            // Set the Content-Length header
            std::string length_field = std::to_string(content_length);
            buffer_body_response.insert(beast::http::field::content_length, length_field);

            // Get the file MD5 and set the Content-MD5 header
            auto md5 = irods::experimental::filesystem::client::data_object_checksum(conn, path);
            buffer_body_response.insert("Content-MD5", md5);

            // Get the last write time and set the Last-Mofified header
            auto last_write_time__time_point = irods::experimental::filesystem::client::last_write_time(conn, path);
            std::time_t last_write_time__time_t = std::chrono::system_clock::to_time_t(last_write_time__time_point);
            std::string last_write_time__str = irods::s3::api::common_routines::convert_time_t_to_str(last_write_time__time_t, date_format);
            buffer_body_response.insert(beast::http::field::last_modified, last_write_time__str);

            irods::experimental::io::client::default_transport xtrans{conn};
            irods::experimental::io::idstream d{
                xtrans, path, irods::experimental::io::root_resource_name{irods::s3::get_resource()}};

            // seek to the start range
            d.seekg(range_start);
            size_t offset = range_start;

            if (d.fail() || d.bad()) {
                log::error("{}: Fail/badbit set", __FUNCTION__);
                buffer_body_response.result(beast::http::status::forbidden);
                buffer_body_response.body().more = false;
                log::debug("{}: returned {}", __FUNCTION__, buffer_body_response.reason());
                session_ptr->send(std::move(buffer_body_response)); 
                return;
            }
            beast::error_code ec;
            beast::http::write_header(session_ptr->stream().socket(), serializer, ec);
            if (ec) {
                buffer_body_response.result(beast::http::status::internal_server_error);
                log::debug("{}: returned {}", __FUNCTION__, buffer_body_response.reason());
                session_ptr->send(std::move(buffer_body_response)); 
                return;
            }

            // TODO Break this up into chunks and after each chunk is sent, schedule a background
            // task to send another chunk, and return from this background task.  This will allow
            // equal access for all clients.
            std::streampos current, size;
            while (d.good()) {
                buffer_body_response.result(beast::http::status::ok);

                // Determine the length we need to read which is the smaller
                // of the write_buffer_size or the bytes to the end of the range.
                // Note that ranges are inclusive which is why the +1's exist. 
                std::size_t read_length
                    = write_buffer_size < range_end + 1 - offset 
                    ? write_buffer_size 
                    : range_end + 1 - offset;

                d.read(buf_vector.data(), read_length);
                current = d.gcount();
                offset += current;
                size += current;
                buffer_body_response.body().data = buf_vector.data();
                buffer_body_response.body().size = current;
                if (d.bad()) {
                    // An error occurred on reading from iRODS.  We have already sent
                    // the response in the header.  All we can do is bail.
                    log::error("{}: Badbit set on read from iRODS.  Bailing...", __FUNCTION__);
                    return;
                }
                beast::http::write(session_ptr->stream().socket(), serializer, ec);
                if (ec == beast::http::error::need_buffer) {
                    ec = {};
                } else if (ec) {
                    // An error occurred writing the body data.  We have already sent
                    // the response in the header.  All we can do is bail.
                    log::error("{}: Error {} occurred while sending socket data.  Bailing...", __FUNCTION__, ec.message());
                    return;
                }

                // If we have now read beyond the range_end then we are done.  Break out.
                if (offset > range_end) {
                    break;
                }
            }
            buffer_body_response.body().size = d.gcount();
            log::debug("{}: Write {} bytes total", __FUNCTION__, size);
            buffer_body_response.body().more = false;
            log::debug("{}: returned {} error={}", __FUNCTION__, buffer_body_response.reason(), ec.message());
            return;
        }
        else {
            log::debug("{}: Could not find file", __FUNCTION__);
            response.result(boost::beast::http::status::not_found);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response)); 
            return;
        }
    }
    catch (irods::exception& e) {
        log::error("{}: Exception {}", __FUNCTION__, e.what());

        switch (e.code()) {
            case USER_ACCESS_DENIED:
            case CAT_NO_ACCESS_PERMISSION:
                response.result(beast::http::status::forbidden);
                break;
            default:
                response.result(beast::http::status::internal_server_error);
                break;
        }

        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }
    catch (std::exception& e) {
        log::error("{}: Exception {}", __FUNCTION__, e.what());
        response.result(beast::http::status::internal_server_error);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }
    catch (...) {
        log::error("{}: Unknown exception encountered", __FUNCTION__);
        response.result(beast::http::status::internal_server_error);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }

    // all paths should have a return here
}
