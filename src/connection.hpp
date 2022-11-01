#pragma once
#include "irods/rcConnect.h"
#include <iostream>

namespace irods::s3
{
    namespace __detail
    {
        struct rcComm_Deleter
        {
            rcComm_Deleter() = default;
            constexpr void operator()(rcComm_t* conn) const noexcept
            {
                if (conn == nullptr) {
                    return;
                }
                rcDisconnect(conn);
            }
        };
    } //namespace __detail
    std::unique_ptr<rcComm_t, __detail::rcComm_Deleter> get_connection();

} //namespace irods::s3