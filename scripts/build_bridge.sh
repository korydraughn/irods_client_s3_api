#!/usr/bin/env bash
# Configure

pushd irods_s3_bridge || exit
mkdir build
cd build
cmake .. -DBoost_DIR=/boost_1_81_0/stage/lib/cmake/Boost-1.81.0/ \
  -Dfmt_DIR=/opt/irods-externals/fmt8.1.1-0/lib/cmake/fmt \
  -Dnlohmann_json_DIR=/opt/irods-externals/json3.10.4-0/lib/cmake/nlohmann_json \
  -DCMAKE_CXX_FLAGS="-stdlib=libc++"
make package -j