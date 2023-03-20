#include "hmac.hpp"
#include "hmac_sha256.h"

// This is a very slight hack I guess but it is required to compile successfully
extern "C" {
#include "sha256.h"
}

#include <fmt/core.h>
#include <sstream>
#include <iostream>

namespace irods::s3::authentication
{
    std::string hmac_sha_256(const std::string_view& key, const std::string_view& data)
    {
        unsigned char result[32];
        unsigned int result_size = 32;
        hmac_sha256(key.data(), key.length(), data.data(), data.length(), result, 32);
        std::cout << "HMAC size is " << result_size << " bytes" << std::endl;
        return std::string(std::string_view((char*) result, 32));
    }

    std::string hash_sha_256(const std::string_view& data)
    {
        unsigned char result[32];
        size_t result_size;
        Sha256Context ctx;
        Sha256Initialise(&ctx);
        Sha256Update(&ctx, static_cast<const void*>(data.data()), data.length());
        SHA256_HASH hash;
        Sha256Finalise(&ctx, &hash);
        return std::string((char*) hash.bytes, 32);
    }

    std::string hex_encode(const std::string_view& data)
    {
        std::stringstream s;
        for (int i = 0; i < data.length(); i++) {
            s << fmt::format("{:02x}", (unsigned char) data[i]);
        }
        return s.str();
    }
} //namespace irods::s3::authentication
