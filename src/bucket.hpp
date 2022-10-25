#pragma once

#include "boost/url/segments_view.hpp"
#include <filesystem>

namespace irods::s3
{
    std::filesystem::path resolve_bucket(const boost::urls::segments_view&);
} //namespace irods::s3
