#!/bin/bash

source /etc/profile
find /data/bloomfilter_db/ -mtime +15 -name "*.bloomfilter" -exec rm -rf {} \;
