#!/bin/bash
source /etc/profile

############  example ##################
#distinct_server.sh -DdataDir=/data/bloomfilter_db -Dport=
########################################
params=$@

cmd="java  -cp ../target/distinct-server.jar:$ANA_HOME $params com.github.distinct_server.Server" 
echo '-----------------------------------------------'
echo $cmd
echo '-----------------------------------------------'

$cmd 


