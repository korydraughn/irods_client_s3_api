#include "hmac.hpp"
#include <openssl/hmac.h>
#include <openssl/evp.h>

#include <fmt/core.h>

namespace irods::s3::authentication
{
    std::string hmac_sha_256(const std::string_view& key, const std::string_view& data)
    {
        unsigned char result[512];
        unsigned int result_size;
        HMAC(
            EVP_sha256(),
            (const unsigned char*) key.data(),
            key.length(),
            (const unsigned char*) data.data(),
            data.length(),
            result,
            &result_size);
            
        return std::string((char*) result, result_size);
    }
} //namespace irods_s3_bridge::authentication