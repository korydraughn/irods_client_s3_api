#!/bin/bash

# Add the package sources
sudo apt-get update
sudo apt install -y irods-dev irods-externals-\* irods-icommands

# log in so we can grab the up to date irods lol
# This is a temporary measure necessary until 4.3.1 is released
#
# You should start cutting here after that has been released
cat bison_credentials | iinit

# Grab the temporary files.
iget -r  /tempZone/home/rods/ub20packages_for_violet/ubuntu-20.04
dpkg -i ubuntu-20.04/irods-server_4.3.0-1~focal_amd64.deb
dpkg -i ubuntu-20.04/irods-runtime_4.3.0-1~focal_amd64.deb
dpkg -i ubuntu-20.04/irods-dev_4.3.0-1~focal_amd64.deb
dpkg -i ubuntu-20.04/irods-database-plugin-postgres_4.3.0-1~focal_amd64.deb
# And here's where you can stop cutting :)


