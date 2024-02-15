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

#include <boost/beast/core/error.hpp>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/read.hpp>

#include <iostream>
#include <vector>
#include <fstream>
#include <regex>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace fs = irods::experimental::filesystem;
namespace log = irods::http::log;

static std::regex upload_id_pattern("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

void irods::s3::actions::handle_putobject(
    irods::http::session_pointer_type session_ptr,
    beast::http::request_parser<boost::beast::http::empty_body>& empty_body_parser,
    const boost::urls::url_view& url)
{
    beast::http::response<beast::http::empty_body> response;

    boost::beast::error_code ec;

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

    // change the parser to a buffer_body parser
    beast::http::request_parser<boost::beast::http::buffer_body> parser{std::move(empty_body_parser)};
    auto& parser_message = parser.get();

    // Look for the header that MinIO sends for chunked data.  If it exists we
    // have to parse chunks ourselves.
    bool special_chunked_header = false;
    auto header = parser_message.find("x-Amz-Content-Sha256");
    if (header != parser_message.end() && header->value() == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") {
        special_chunked_header = true;
    }
    log::debug("{} special_chunk_header: {}", __FUNCTION__, special_chunked_header);

    // See if we have chunked set.  If so turn off special chunked header flag as the
    // parser will handle it.
    bool chunked_flag = false;
    if (parser_message[beast::http::field::transfer_encoding] == "chunked") {
        special_chunked_header = false;
        chunked_flag = true;
    }

    // If chunked_flag is false, make sure we have Content-Length or
    // we will not be able to parse the response as the parser will return
    // immediately.  If we have special_chunked header we still need
    // the Content-Length.
    if (!chunked_flag) {
        header = parser_message.find("Content-Length");
        if (header == parser_message.end()) {
            log::error("{} Neither Content-Length nor chunked mode were set.", __FUNCTION__);
            response.result(boost::beast::http::status::bad_request);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        }
    }

    if (parser_message[beast::http::field::expect] == "100-continue") {
        response.version(11);
        response.result(beast::http::status::continue_);
        response.set(beast::http::field::server, parser_message["Host"]);
        session_ptr->send(std::move(response)); 
        log::debug("{}: Sent 100-continue", __FUNCTION__);
    }

    fs::path path;
    if (auto bucket = irods::s3::resolve_bucket(conn, url.segments()); bucket.has_value()) {
        path = std::move(bucket.value());
        path = irods::s3::finish_path(path, url.segments());
    }
    else {
        log::error("{}: Failed to resolve bucket", __FUNCTION__);
        response.result(beast::http::status::not_found);
        log::debug("{}: returned {}", __FUNCTION__, response.reason());
        session_ptr->send(std::move(response)); 
        return;
    }
    log::debug("{}: Path [{}]", __FUNCTION__, path.string());

    // check to see if this is a part upload
    bool upload_part = false;
    std::string part_number;
    std::string upload_id;
    std::string upload_part_filename;
    if (const auto part_number_param = url.params().find("partNumber"); part_number_param != url.params().end()) {
        part_number = (*part_number_param).value;
    }
    if (const auto upload_id_param = url.params().find("uploadId"); upload_id_param != url.params().end()) {
        upload_id = (*upload_id_param).value;
    }

    if (!part_number.empty() || !upload_id.empty()) {
        // either partNumber or uploadId provided
        upload_part = true;
        if (part_number.empty()) {
            log::error("{}: UploadPart detected but partNumber was not provided.", __FUNCTION__);
            response.result(beast::http::status::bad_request);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        } else if (upload_id.empty()) {
            log::error("{}: UploadPart detected but upload_id was not provided.", __FUNCTION__);
            response.result(beast::http::status::bad_request);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        } else if (!std::regex_match(upload_id, upload_id_pattern)) {
            log::error("{}: Upload ID {} was not in expected format.", __FUNCTION__, upload_id);
            response.result(beast::http::status::bad_request);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response)); 
            return;
        }

        upload_part_filename = upload_id + "." + part_number;
        log::debug("{}: UploadPart detected.  partNumber={} uploadId={}", __FUNCTION__, part_number, upload_id);
    }

    irods::experimental::io::client::default_transport xtrans{conn};
    fs::client::create_collections(conn, path.parent_path());

    std::ofstream ofs;                    // posix file stream for writing parts
    irods::experimental::io::odstream d;  // irods dstream for writing directly to irods

    if (upload_part) {
        ofs.open(upload_part_filename, std::ofstream::out);
        if (!ofs.is_open()) {
            log::error("{}: Failed to open stream for writing part", __FUNCTION__);
            response.result(beast::http::status::internal_server_error);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        }
    } else {
        d.open(xtrans, path, irods::experimental::io::root_resource_name{irods::s3::get_resource()}, std::ios_base::out);
        if (!d.is_open()) {
            log::error("{}: Failed to open dstream to iRODS", __FUNCTION__);
            response.result(beast::http::status::internal_server_error);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        }
    }

    uint64_t read_buffer_size = irods::s3::get_put_object_buffer_size_in_bytes();
    log::debug("{}: read_buffer_size={}", __FUNCTION__, read_buffer_size);

    std::vector<char> buf_vector(read_buffer_size);
    parser_message.body().data = buf_vector.data();
    parser_message.body().size = read_buffer_size; 

    response.set("Etag", path.c_str());
    response.set("Connection", "close");

    if (special_chunked_header) {
        log::debug("{}: read_buffer_size={}", __FUNCTION__, read_buffer_size);

        boost::beast::error_code ec;
        bool need_more = true;

        enum class parsing_state {
            header_begin, header_continue, body, end_of_chunk, parsing_done, parsing_error
        };

        parsing_state current_state = parsing_state::header_begin;

        // This is a string that holds input bytes temporarily as they are being read
        // from the stream.  This chunk parser will only read more bytes into this
        // when necessary to continue parsing.  Once bytes are no longer needed they are
        // discarded from this variable.
        std::string the_bytes;

        size_t chunk_size = 0;

        // While we have more data to be read from the socket into the_bytes or
        // we haven't exhausted the_bytes.
        while (!parser.is_done() || !the_bytes.empty()) {

            // check to see if we are done parsing the chunks
            if (current_state == parsing_state::parsing_done) {
                break;
            }

            parser_message.body().data = buf_vector.data();
            parser_message.body().size = read_buffer_size;
            
            if (!parser.is_done() && need_more) {

                // The eager option instructs the parser to continue reading the buffer once it has completed a
                // structured element (header, chunk header, chunk body). Since we are handling the parsing ourself,
                // we want the parser to give us as much as is available.
                parser.eager(true);
                beast::http::read_some(session_ptr->stream().socket(), session_ptr->get_buffer(), parser, ec);

                if(ec == beast::http::error::need_buffer) {
                    // Need buffer indicates the current buffer was filled (new size value is zero).
                    // We read the bytes on each iteration and reset the buffer so no action is needed. 
                    ec = {};
                }
                if(ec) {
                    log::error("{}: error reading data", __FUNCTION__);
                    response.result(beast::http::status::internal_server_error);
                    log::debug("{}: returned {}", __FUNCTION__, response.reason());
                    session_ptr->send(std::move(response));
                    return;
                }

                size_t read_bytes = read_buffer_size - parser_message.body().size;
                the_bytes.append(buf_vector.data(), read_bytes);

                // Don't get more bytes until we detect we need them.
                need_more = false;
            }

            switch (current_state) {
                case parsing_state::header_begin:
                {
                    // Chunk headers can be of the following forms:
                    // 1. With extensions: <hex>;extension1=extension1value;extension2=extension2value\r\n
                    // 2. Without extensions: <hex>\r\n
                    
                    // See if it is of form 1.
                    size_t semicolon_location = the_bytes.find (";");
                    if (semicolon_location == std::string::npos) {

                        // Not form 1.  See if it is form 2.
                        size_t newline_location = the_bytes.find("\r\n");
                        if (newline_location != std::string::npos) {
                            std::string chunk_size_str = the_bytes.substr(0, newline_location);
                            size_t hex_digits_parsed = 0;
                            try {
                                chunk_size = stoull(chunk_size_str, &hex_digits_parsed, 16);
                            } catch (std::invalid_argument& e) {
                                hex_digits_parsed = 0;
                            }

                            if (hex_digits_parsed == chunk_size_str.length()) {
                                // eat the bytes up to and including \r\n 
                                the_bytes = the_bytes.substr(newline_location+2);
                                if (chunk_size == 0) {
                                    current_state = parsing_state::end_of_chunk;
                                } else {
                                    current_state = parsing_state::body;
                                }
                            } else {
                                log::error("{}: bad chunk size: {}", __FUNCTION__, chunk_size_str);
                                current_state = parsing_state::parsing_error;
                            }
                        } else if (!parser.is_done()) {
                            need_more = true;
                        } else {
                            // we have received all of the bytes but do not have "<hex>\r\n" sequence
                            log::error("{}: Malformed chunk header", __FUNCTION__);
                            current_state = parsing_state::parsing_error;
                        }
                    } else {
                        // semicolon found
                        std::string chunk_size_str = the_bytes.substr(0, semicolon_location);
                        size_t hex_digits_parsed = 0;
                        try {
                           chunk_size = stoull(chunk_size_str, &hex_digits_parsed, 16);
                        } catch (std::invalid_argument& e) {
                            hex_digits_parsed = 0;
                        }
                        if (hex_digits_parsed == chunk_size_str.length()) {
                            // eat the bytes up to and including semicolon
                            the_bytes = the_bytes.substr(semicolon_location+1);
                            current_state = parsing_state::header_continue;
                        } else {
                            log::error("{}: bad chunk size: {}", __FUNCTION__, chunk_size_str);
                            current_state = parsing_state::parsing_error;
                        }
                    }
                    break;
                }
                case parsing_state::header_continue:
                {
                    // move beyond the newline 
                    size_t newline_location = the_bytes.find("\r\n");

                    // reset string stream and set it to after the \r\n
                    if (newline_location != std::string::npos) {
                        the_bytes = the_bytes.substr(newline_location+2);
                        if (chunk_size == 0) {
                            current_state = parsing_state::parsing_done;
                        } else {
                            current_state = parsing_state::body;
                        }
                    } else if (!parser.is_done()) {
                        need_more = true;
                    } else {
                        // we have read all the bytes but do not have a \r\n at the end
                        // of the header line
                        log::error("{}: Malformed chunk header", __FUNCTION__);
                        current_state = parsing_state::parsing_error;
                    }
                    break;
                }
                case parsing_state::body:
                {
                    // if we have the full chunk, handle it otherwise continue
                    // until we do
                    if (the_bytes.length() >= chunk_size) {
                        std::string body = the_bytes.substr(0, chunk_size);

                        try {
                            if (upload_part) {
                                ofs.write(body.c_str(), body.length());
                            } else {
                                d.write(body.c_str(), body.length());
                            }
                        }
                        catch (std::exception& e) {
                            log::error("{}: Exception when writing to file - {}", __FUNCTION__, e.what());
                            response.result(beast::http::status::internal_server_error);
                            log::debug("{}: returned {}", __FUNCTION__, response.reason());
                            session_ptr->send(std::move(response));
                            return;
                        }

                        // reset string stream and set it to after the \r\n
                        the_bytes = the_bytes.substr(chunk_size);
                        current_state = parsing_state::end_of_chunk;
                    } else {
                        // Note to save memory we could stream the bytes we have, decrease
                        // chunk size, and discard these bytes.
                        need_more = true;
                    }
                    break;
                }
                case parsing_state::end_of_chunk:
                {
                    // If the size of the_bytes is just one and consists of "\r"
                    // then we need to get more bytes.
                    if (!parser.is_done() && the_bytes.size() == 1 && the_bytes[0] == '\r') {
                        // we don't have enough bytes to read the expected "\r\n"
                        // but there are more bytes to be read
                        need_more = true;
                        break;
                    }

                    // If the size is 0 then we need to get more bytes.
                    if (!parser.is_done() && the_bytes.size() == 0) {
                        // we don't have enough bytes to read the expected "\r\n"
                        // but there are more bytes to be read
                        need_more = true;
                        break;
                    }

                    size_t newline_location = the_bytes.find("\r\n");
                    if (newline_location != 0) {
                        log::error("{}: Invalid chunk end sequence", __FUNCTION__);
                        current_state = parsing_state::parsing_error;
                    } else {
                        // remove \r\n and go to next chunk
                        the_bytes = the_bytes.substr(2);
                        current_state = parsing_state::header_begin;
                    }
                    break;
                }
                default:
                    break;

            };

        }

        if (current_state == parsing_state::parsing_error) {
            log::error("{}: Error parsing chunked body", __FUNCTION__);
            response.result(boost::beast::http::status::bad_request);
            log::debug("{}: returned {}", __FUNCTION__, response.reason());
            session_ptr->send(std::move(response));
            return;
        }

    } else {
        // Let boost::beast::parser handle the body
        while (!parser.is_done()) {
            parser_message.body().data = buf_vector.data();
            parser_message.body().size = read_buffer_size;
            beast::http::read_some(session_ptr->stream(), session_ptr->get_buffer(), parser, ec);

            // Before read, the parser_message.body().size is the size of the buffer.
            // After read, parser_message.body().size is the size of the part of the 
            // buffer that was used/read into.  So, the number of bytes read is the buffer size
            // minus the new parser_message.body().size.
            size_t read_bytes = read_buffer_size - parser_message.body().size;

            std::string the_bytes{buf_vector.data(), read_bytes};

            if (ec == beast::http::error::need_buffer) {
                // Need buffer indicates the current buffer was filled (new size value is zero).
                // We read the bytes on each iteration and reset the buffer so no action is needed. 
                ec = {};
            }

            if (ec) {
                log::error("{}: Error when parsing file - {}", __FUNCTION__, ec.what());
                response.result(beast::http::status::internal_server_error);
                log::debug("{}: returned {}", __FUNCTION__, response.reason());
                session_ptr->send(std::move(response));
                return;
            }

            try {
                if (upload_part) {
                    ofs.write((char*) buf_vector.data(), read_bytes);
                } else {
                    d.write((char*) buf_vector.data(), read_bytes);
                }
            }
            catch (std::exception& e) {
                log::error("{}: Exception when writing to file - {}", __FUNCTION__, e.what());
                response.result(beast::http::status::internal_server_error);
                log::debug("{}: returned {}", __FUNCTION__, response.reason());
                session_ptr->send(std::move(response));
                return;
            }

         }
    }

    response.result(beast::http::status::ok);
    log::debug("{}: returned {}", __FUNCTION__, response.reason());
    session_ptr->send(std::move(response)); 
    return;
}
