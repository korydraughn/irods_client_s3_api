#pragma once
#include <string_view>
#include <string>
#include <vector>

namespace irods_s3_bridge::authentication
{
    std::string hmac_sha_256(const std::string_view& key, const std::string_view& data);
} //namespace irods_s3_bridge::authentication