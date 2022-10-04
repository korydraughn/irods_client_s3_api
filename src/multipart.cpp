#include "persistence.hpp"
#include "multipart.hpp"
#include <cctype>
#include <sstream>
#include <fmt/format.h>

// The way that multipart is going to work here is that they will exist as directories(collections fine)
// in the irods server under a temporary directory.
//
// The path of the finished object needs to be able to be encoded to a data-object name safe string.
// It does, however, not need to be decoded from the other end.

std::string_view temporary_collection = "/tempZone/temp";
namespace irods::s3::multipart::utilities
{
    // In order to store the entire path in the bucket as a file, it makes a lot of sense
    // to encode the eventual path of the product data object.
    //
    // Given that it doesn't really do much for our purpose to have it decoded, it should be fine that this is
    // a relatively unpleasant pattern to parse.
    //
    // This may be entirely identical to encode_uri in authentication.cpp
    const std::string encode_path(const std::string_view& sv)
    {
        std::stringstream s;
        std::ios default_state(nullptr);
        default_state.copyfmt(s);
        auto pick_me = [&](char c) {
            if (std::isgraph(c) && c != '%') {
                s << std::setw(0) << c;
            }
            else {
                if (c != '%') {
                    s << std::setw(0) << "%" << std::hex << std::setfill('0') << std::setw(2)
                      << static_cast<uint8_t>(c);
                    s.copyfmt(default_state);
                }
                else {
                    s << "%%";
                }
            }
        };
        std::for_each(sv.begin(), sv.end(), pick_me);
        return s.str();
    }

    const std::string get_temporary_file(const std::string_view& path, const std::string_view& part_number)
    {
        return fmt::format("{}/{}/{}", temporary_collection, encode_path(path), part_number);
    }
} //namespace irods::s3::multipart::utilities
