#!/bin/bash

# Add the package sources
#wget -qO - https://packages.irods.org/irods-signing-key.asc | sudo apt-key add -
#echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | sudo tee /etc/apt/sources.list.d/renci-irods.list
sudo apt-get update
sudo apt install -y irods-dev irods-externals-\* irods-icommands

# log in so we can grab the up to date irods lol
cat bison_credentials | iinit

# Grab the temporary files.
iget -r  /tempZone/home/rods/ub20packages_for_violet/ubuntu-20.04
dpkg -i ubuntu-20.04/irods-server_4.3.0-1~focal_amd64.deb
dpkg -i ubuntu-20.04/irods-runtime_4.3.0-1~focal_amd64.deb
dpkg -i ubuntu-20.04/irods-dev_4.3.0-1~focal_amd64.deb
dpkg -i ubuntu-20.04/irods-database-plugin-postgres_4.3.0-1~focal_amd64.deb

# Clone the repository
#git clone --recursive https://github.com/epsilon-phase/irods_client_s3_cpp

# Gather some dependencies

# We might not need this dependency
#git clone --branch v3.11.2 https://github.com/nlohmann/json

# We definitely need a newer boost than we have
#wget https://boostorg.jfrog.io/artifactory/main/release/1.81.0/source/boost_1_81_0.tar.bz2
#tar xaf boost_1_81_0.tar.bz2
#pushd boost_1_81_0 > /dev/null || exit
#./bootstrap.sh
# Turns out passing -j isn't enough, we need to detect the number of processors to use.
#./b2 -j `nproc` link=static
#popd > /dev/null || exit


