#!/usr/bin/bash
ldconfig
sleep 10

cd ~
irods_s3_bridge
while true; do
  sleep 1;
done