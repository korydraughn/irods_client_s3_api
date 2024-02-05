#include "irods/private/s3_api/session.hpp"

#include "irods/private/s3_api/globals.hpp"
#include "irods/private/s3_api/log.hpp"
#include "irods/private/s3_api/s3_api.hpp"

#include <boost/beast/version.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/config.hpp>
#include <boost/url/src.hpp>

#include <nlohmann/json.hpp>

#include <chrono>
#include <iterator>
#include <utility>

#ifdef IRODS_WRITE_REQUEST_TO_TEMP_FILE
#  include <fstream>
#endif

namespace irods::http
{
	session::session(
		boost::asio::ip::tcp::socket&& socket,
		const request_handler_map_type& _request_handler_map,
		int _max_body_size,
		int _timeout_in_seconds)
		: stream_(std::move(socket))
		, req_handlers_{&_request_handler_map}
		, max_body_size_{_max_body_size}
		, timeout_in_secs_{_timeout_in_seconds}
	{
	} // session (constructor)

	auto session::ip() const -> std::string
	{
		return stream_.socket().remote_endpoint().address().to_string();
	} // ip

	// Start the asynchronous operation
	auto session::run() -> void
	{
		// We need to be executing within a strand to perform async operations
		// on the I/O objects in this session. Although not strictly necessary
		// for single-threaded contexts, this example code is written to be
		// thread-safe by default.
		boost::asio::dispatch(
			stream_.get_executor(), boost::beast::bind_front_handler(&session::do_read, shared_from_this()));
	} // run

	auto session::do_read() -> void
	{
		// Construct a new parser for each message.
		parser_.emplace();

		// Apply the limit defined in the configuration file.
		parser_->body_limit(max_body_size_);

		// Set the timeout.
		stream_.expires_after(std::chrono::seconds(timeout_in_secs_));

		// Read a request.
		boost::beast::http::async_read(
			stream_, buffer_, *parser_, boost::beast::bind_front_handler(&session::on_read, shared_from_this()));
	} // do_read

	auto session::on_read(boost::beast::error_code ec, std::size_t bytes_transferred) -> void
	{
		boost::ignore_unused(bytes_transferred);

		// This means they closed the connection
		if (ec == boost::beast::http::error::end_of_stream) {
			return do_close();
		}

		if (ec == boost::beast::http::error::body_limit) {
			logging::error("{}: Request constraint error: {}", __func__, ec.message());
			return;
		}

		if (ec) {
			return irods::fail(ec, "read");
		}

		//
		// Process client request and send a response.
		//

        // TODO the parser probably is invalidated?
		auto& req_ = parser_->get();

		// Print the headers.
		for (auto&& h : req_.base()) {
			logging::debug("{}: Header: ({}, {})", __func__, h.name_string(), h.value());
		}

		// Print the components of the request URL.
		logging::debug("{}: Method: {}", __func__, req_.method_string());
		logging::debug("{}: Version: {}", __func__, req_.version());
		logging::debug("{}: Target: {}", __func__, req_.target());
		logging::debug("{}: Keep Alive: {}", __func__, req_.keep_alive());
		logging::debug("{}: Has Content Length: {}", __func__, req_.has_content_length());
		logging::debug("{}: Chunked: {}", __func__, req_.chunked());
		logging::debug("{}: Needs EOF: {}", __func__, req_.need_eof());

        std::string url_string = static_cast<std::string>(req_.find("Host")->value()).append(req_.target());
		log::debug("{}: Candidate url string: {}", __func__, url_string);

		namespace http = boost::beast::http;

        (void)req_handlers_;
        boost::urls::url url2;
        auto host = req_.find("Host")->value();
        url2.set_encoded_host(host.find(':') != std::string::npos ? host.substr(0, host.find(':')) : host);
        url2.set_path(req_.target().substr(0, req_.target().find("?")));
        url2.set_scheme("http");
        if (req_.target().find('?') != std::string::npos) {
            url2.set_encoded_query(req_.target().substr(req_.target().find("?") + 1));
        }

        boost::urls::url_view url = url2;
        const auto& segments = url.segments();
        const auto& params = url.params();

        switch (req_.method()) {
            case boost::beast::http::verb::get:
                if (segments.empty() || params.contains("encoding-type") || params.contains("list-type")) {
                    auto f = params.find("list-type");

                    if (f != params.end() && (*f).value == "2") {
		                log::debug("{}: ListObjects detected", __func__);
                        boost::beast::http::response<boost::beast::http::string_body> response;
                        auto shared_this = shared_from_this();
		                irods::http::globals::background_task(
		                	[shared_this, &parser = this->parser_, _url = std::move(url), _response = std::move(response) ]() mutable {
                            irods::s3::actions::handle_listobjects_v2(shared_this, *parser, _url, _response);
			            });
                        return;
                    }
                }
                else {
                    if (req_.target() == "/") {
		                log::debug("{}: ListBuckets detected", __func__);
                        boost::beast::http::response<boost::beast::http::string_body> response;
                        auto shared_this = shared_from_this();
		                irods::http::globals::background_task(
		                	[shared_this, &parser = this->parser_, _url = std::move(url), _response = std::move(response) ]() mutable {
                            irods::s3::actions::handle_listbuckets(shared_this, *parser, _url, _response);
			            });
                        return;
                    }
                  /*  else if (params.find("location") != params.end()) {
                        // This is GetBucketLocation
                        std::cout << "GetBucketLocation detected" << std::endl;
                        std::string s3_region = irods::s3::get_s3_region();
                        beast::http::response<beast::http::string_body> resp;
                        boost::property_tree::ptree document;
                        document.add("LocationConstraint", s3_region);
                        std::stringstream s;
                        boost::property_tree::xml_parser::xml_writer_settings<std::string> settings;
                        settings.indent_char = ' ';
                        settings.indent_count = 4;
                        boost::property_tree::write_xml(s, document, settings);
                        resp.body() = s.str();
                        co_await beast::http::async_write(socket, resp, asio::use_awaitable);
                        co_return;
                    }
                    else if (params.find("object-lock") != params.end()) {
                        std::cout << "GetObjectLockConfiguration detected" << std::endl;
                        beast::http::response<beast::http::string_body> resp;
                        resp.body() = "<?xml version='1.0' encoding='utf-8'?>"
                                      "<ObjectLockConfiguration/>";
                        co_await beast::http::async_write(socket, resp, asio::use_awaitable);
                        co_return;
                    }
                    else {
                        std::cout << "getobject detected" << std::endl;
                        co_await irods::s3::actions::handle_getobject(socket, parser, url);
                    }*/
                }
                break;
            /*case boost::beast::http::verb::put:
                if (req_.find("x-amz-copy-source") != req_.end()) {
                    // copyobject
                    std::cout << "Copyobject detected" << std::endl;
                    co_await irods::s3::actions::handle_copyobject(socket, parser, url);
                }
                else {
                    // putobject
                    std::cout << "putobject detected" << std::endl;
                    co_await irods::s3::actions::handle_putobject(socket, parser, url);
                    co_return;
                }
                break;
            case boost::beast::http::verb::delete_:
                if (segments.empty()) {
                    std::cout << "Deletebucket detected?" << std::endl;
                }
                else {
                    std::cout << "Deleteobject detected" << std::endl;
                    co_await irods::s3::actions::handle_deleteobject(socket, parser, url);
                    co_return;
                }
                break;*/
            case boost::beast::http::verb::head:
                // Determine if it is HeadBucket or HeadObject 
                if (1 == segments.size()) {
		            log::debug("{}: HeadBucket detected", __func__);
                    boost::beast::http::response<boost::beast::http::string_body> response;
                    auto shared_this = shared_from_this();
		            irods::http::globals::background_task(
		            	[shared_this, &parser = this->parser_, _url = std::move(url), _response = std::move(response) ]() mutable {
                        irods::s3::actions::handle_headbucket(shared_this, *parser, _url, _response);
			        });
                    return;
                }
                else {
		            log::debug("{}: HeadObject detected", __func__);
                    boost::beast::http::response<boost::beast::http::string_body> response;
                    auto shared_this = shared_from_this();
		            irods::http::globals::background_task(
		            	[shared_this, &parser = this->parser_, _url = std::move(url), _response = std::move(response) ]() mutable {
                        irods::s3::actions::handle_headobject(shared_this, *parser, _url, _response);
			        });
                    return;
                }
                break;
            default:
		        log::error("{}: Someone tried to make an HTTP request with a method that is not yet supported", __func__);
        }

        send(irods::http::fail(http::status::ok));

		/*try {
#ifdef IRODS_WRITE_REQUEST_TO_TEMP_FILE
			std::ofstream{"/tmp/http_request.txt"}.write(req_.body().c_str(), (std::streamsize) req_.body().size());
#endif

			// "host" is a placeholder that's used so that get_url_path() can parse the URL correctly.
			const auto path = irods::http::get_url_path(fmt::format("http://host{}", req_.target()));
			if (!path) {
				send(irods::http::fail(http::status::bad_request));
				return;
			}

			if (const auto iter = req_handlers_->find(*path); iter != std::end(*req_handlers_)) {
				(iter->second)(shared_from_this(), req_);
				return;
			}

			send(irods::http::fail(http::status::not_found));
		}
		catch (const std::exception& e) {
			logging::error("{}: {}", __func__, e.what());
			send(irods::http::fail(http::status::internal_server_error));
		}*/
	} // on_read

	auto session::on_write(bool close, boost::beast::error_code ec, std::size_t bytes_transferred) -> void
	{
		boost::ignore_unused(bytes_transferred);

		if (ec) {
			return irods::fail(ec, "write");
		}

		if (close) {
			// This means we should close the connection, usually because
			// the response indicated the "Connection: close" semantic.
			return do_close();
		}

		// We're done with the response so delete it
		res_ = nullptr;

		// Read another request
		do_read();
	} // on_write

	auto session::do_close() -> void
	{
		// Send a TCP shutdown.
		boost::beast::error_code ec;
		stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);

		// At this point the connection is closed gracefully.
	} // do_close
} // namespace irods::http
