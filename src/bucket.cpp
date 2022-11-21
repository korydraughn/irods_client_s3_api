#include <filesystem>
#include "./bucket.hpp"

namespace irods::s3
{
    // TODO this could be pluggable in the future,
    std::filesystem::path resolve_bucket(rcComm_t&,const boost::urls::segments_view& view)
    {
        std::filesystem::path p;
        p = "/tempZone/home/rods";
        for (const auto& i : view)
            p /= i;
        return p;
    }
    std::string strip_bucket(const std::string& a)
    {
        return a;
    }

} //namespace irods::s3
