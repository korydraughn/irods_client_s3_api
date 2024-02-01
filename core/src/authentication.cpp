#include "irods/private/s3_api/authentication.hpp"
#include "irods/private/s3_api/hmac.hpp"
#include "irods/private/s3_api/log.hpp"
#include "irods/private/s3_api/log.hpp"

#include <algorithm>
#include <iostream>
#include <iomanip>
#include <sstream>

#include <boost/algorithm/string.hpp>

#include <irods/rcMisc.h>
#include <irods/rodsKeyWdDef.h>

namespace
{

    std::string uri_encode(const std::string_view sv)
    {
        std::stringstream s;
        std::ios state(nullptr);
        state.copyfmt(s);
        for (auto c : sv) {
            bool encode = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                          boost::is_any_of("-_~.")(c);
            if (!encode) {
                // Interestingly, most hex-encoded values in the amazon api tend to be lower case,
                // except for this.
                s << '%' << std::uppercase << std::hex << std::setw(2) << std::setfill('0') << (int) c;
                s.copyfmt(state);
            }
            else {
                s << c;
            }
        }
        return s.str();
    }

    std::string
    get_user_signing_key(const std::string_view secret_key, const std::string_view date, const std::string_view region)
    {
        namespace log = irods::http::log;
        // Hate it when amazon gives me homework
        // during implementation of their protocol
        log::debug("date time component is {}", date);
        auto date_key = irods::s3::authentication::hmac_sha_256(std::string("AWS4").append(secret_key), date);
        auto date_region_key = irods::s3::authentication::hmac_sha_256(date_key, region);
        // 'date region service key'
        auto date_region_service_key = irods::s3::authentication::hmac_sha_256(date_region_key, "s3");
        return irods::s3::authentication::hmac_sha_256(date_region_service_key, "aws4_request");
    }

    // TODO when this is working, we can improve performance by reusing the same stringstream where possible.

    // Turn the url into the 'canon form'
    std::string canonicalize_url(const boost::urls::url_view& url)
    {
        std::stringstream result;
        for (const auto& i : url.segments()) {
            result << '/' << uri_encode(i); // :shrug:
        }
        return result.str();
    }

    std::string to_lower(const std::string_view sv)
    {
        std::string r;
        for (auto i : sv) {
            r.push_back(tolower(i));
        }
        return r;
    }
    std::string canonicalize_request(
        const static_buffer_request_parser& request,
        const boost::urls::url_view& url,
        const std::vector<std::string>& signed_headers)
    {
        namespace log = irods::http::log;

        // At various points the signature process wants various fields to be sorted.
        // so reusing this can at least avoid some of the duplicate allocations and such
        std::vector<std::string_view> sorted_fields;

        std::stringstream result;

        std::ios state(nullptr);
        state.copyfmt(result);

        result << request.get().method_string() << '\n';
        result.copyfmt(result); // re store former formatting
        result << canonicalize_url(url) << '\n';
        // Canonicalize query string
        {
            bool first = true;
            // Changing it to a pair enables us to sort easily.
            std::vector<std::pair<std::string, std::string>> params;
            std::transform(
                url.encoded_params().begin(), url.encoded_params().end(), std::back_inserter(params), [](const auto &a) {
                    if (a.has_value) {
                        return std::pair<std::string, std::string>(a.key, a.value);
                    }
                    return std::pair<std::string, std::string>(a.key, "");
                });
            std::sort(params.begin(), params.end());
            for (const auto& param : params) {
                result << (first ? "" : "&") << param.first;
                result << '=' << param.second;

                first = false;
            }
        }
        result << '\n';

        for (const auto& header : request.get()) {
            if (std::find(signed_headers.begin(), signed_headers.end(), to_lower(header.name_string())) !=
                    signed_headers.end())
            {
                sorted_fields.emplace_back(header.name_string().data(), header.name_string().length());
            }
        }

        // Produce the 'canonical headers'

        // They mix cases of headers :)
        // I hate it
        std::sort(sorted_fields.begin(), sorted_fields.end(), [](const auto& lhs, const auto& rhs) {
            const auto result = std::mismatch(
                lhs.cbegin(),
                lhs.cend(),
                rhs.cbegin(),
                rhs.cend(),
                [](const unsigned char lhs, const unsigned char rhs) { return tolower(lhs) == tolower(rhs); });

            return result.second != rhs.cend() &&
                   (result.first == lhs.cend() || tolower(*result.first) < tolower(*result.second));
        });
        std::string fields_str;
        for (auto& i : sorted_fields) {
            fields_str += i;
            fields_str += ",";
        }
        log::debug(fields_str);
        for (const auto& field : sorted_fields) {
            auto val = static_cast<std::string>(request.get().at(boost::string_view(field.data(), field.length())));
            std::string key(field);
            std::transform(key.begin(), key.end(), key.begin(), tolower);
            boost::trim(val);
            result << key << ':';
            result.copyfmt(state);
            result << val << '\n';
        }
        result << "\n";

        sorted_fields.clear();

        // and the signed header list

        for (const auto& hd : signed_headers) {
            sorted_fields.push_back(hd);
        }
        std::sort(sorted_fields.begin(), sorted_fields.end());
        {
            bool first = true;
            for (const auto& i : sorted_fields) {
                result << (first ? "" : ";") << i;
                first = false;
            }
            result.copyfmt(state);
            result << '\n';
        }

        //and the payload signature

        if (auto req = request.get().find("X-Amz-Content-SHA256"); req != request.get().end()) {
            result << req->value();
        }
        else {
            result << "UNSIGNED-PAYLOAD";
        }

        return result.str();
    }

    std::string string_to_sign(
        const static_buffer_request_parser& request,
        const std::string_view date,
        const std::string_view region,
        const std::string_view canonical_request)
    {
        std::stringstream result;
        result << "AWS4-HMAC-SHA256\n";
        result << request.get().at("X-Amz-Date") << '\n';
        result << date << '/' << region << "/s3/aws4_request\n";
        result << irods::s3::authentication::hex_encode(irods::s3::authentication::hash_sha_256(canonical_request));
        return result.str();
    }
} //namespace
std::optional<std::string> irods::s3::authentication::authenticates(
    rcComm_t& conn,
    const static_buffer_request_parser& request,
    const boost::urls::url_view& url)
{
    namespace log = irods::http::log;

    std::vector<std::string> auth_fields, credential_fields, signed_headers;
    // should be equal to something like
    // [ 'AWS4-SHA256-HMAC Credential=...', 'SignedHeaders=...', 'Signature=...']

    boost::split(auth_fields, request.get().at("Authorization"), boost::is_any_of(","));

    // Strip the names and such
    for (auto& field : auth_fields) {
        field = field.substr(field.find('=') + 1);
    }

    // Break up the credential field.
    boost::split(credential_fields, auth_fields[0], boost::is_any_of("/"));

    auto& access_key_id = credential_fields[0]; // This is the username.
    auto& date = credential_fields[1];
    auto& region = credential_fields[2];
    // Look, covering my bases here seems prudent.
    //auto& aws_service = credential_fields[3];

    auto& signature = auth_fields[2];

    boost::split(signed_headers, auth_fields[1], boost::is_any_of(";"));

    auto canonical_request = canonicalize_request(request, url, signed_headers);
    log::debug("========== Canon request ==========\n{}", canonical_request);

    auto sts = string_to_sign(request, date, region, canonical_request);
    log::debug("======== String to sign ===========\n{}", sts);
    log::debug("===================================");

    log::trace("Searching for user with access_key_id={}", access_key_id);
    auto irods_user = irods::s3::authentication::get_iRODS_user(&conn, access_key_id);

    if (!irods_user) {
        log::debug("Authentication Error: No iRODS username mapped to access key ID [{}]", access_key_id);
        return std::nullopt;
    }

    auto signing_key = get_user_signing_key(
        irods::s3::authentication::get_user_secret_key(&conn, access_key_id).value(), date, region);
    auto computed_signature = hex_encode(hmac_sha_256(signing_key, sts));

    log::debug("Computed: [{}]", computed_signature);

    log::debug("Actual Signature: [{}]", signature);

    return (computed_signature == signature) ? irods_user : std::nullopt;
}