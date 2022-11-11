#pragma once
#include <string_view>
#include <nlohmann/json.hpp>
#include <irods/rcConnect.h>

namespace irods::s3::plugins
{
    void load_plugin(rcComm_t& connection, const std::string_view& plugin_name, nlohmann::json& configuration);
} //namespace irods::s3::plugins
