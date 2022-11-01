#include "connection.hpp"
std::unique_ptr<rcComm_t, irods::s3::__detail::rcComm_Deleter> irods::s3::get_connection()
{
    rodsEnv env;
    rErrMsg_t err;
    getRodsEnv(&env);
    std::unique_ptr<rcComm_t, irods::s3::__detail::rcComm_Deleter> result = nullptr;
    // For some reason it isn't working with the assignment operator
    result.reset(rcConnect(env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 0, &err));
    if (result == nullptr || err.status) {
        std::cerr << err.msg << std::endl;
        // Good old code 2143987421 (Manual not consulted due to draftiness, brrr)
        // exit(2143987421);
    }
    if (const int ec = clientLogin(result.get())) {
        std::cout << "Failed to log in" << std::endl;
    }

    return std::move(result);
}