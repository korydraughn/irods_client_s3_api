#include "./bucket.hpp"

namespace irods::s3
{
    std::filesystem::path resolve_bucket(const boost::urls::segments_view& view)
    {
        std::filesystem::path p;
        for (const auto& i : view)
            p /= i;
        return p;
    }
} //namespace irods::s3