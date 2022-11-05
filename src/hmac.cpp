#include "hmac.hpp"
#include <ios>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <fmt/core.h>
#include <sstream>
#include <iostream>

namespace irods::s3::authentication
{
    std::string hmac_sha_256(const std::string_view& key, const std::string_view& data)
    {
        unsigned char result[512];
        unsigned int result_size=0;
        HMAC(
            EVP_sha256(),
            (const unsigned char*) key.data(),
            key.length(),
            (const unsigned char*) data.data(),
            data.length(),
            result,
            &result_size);
        std::cout << "HMAC size is " << result_size << " bytes" << std::endl;
        return std::string(std::string_view((char*) result, 32));
    }
    std::string hash_sha_256(const std::string_view& data)
    {
        unsigned char result[512];
        size_t result_size;

        return std::string(
            std::string_view((char*) SHA256((const unsigned char*) data.data(), data.length(), result), 256 / 8));
    }
} //namespace irods::s3::authentication