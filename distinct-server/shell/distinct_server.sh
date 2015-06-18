#!/bin/bash
source /etc/profile

cur_dir=$(dirname $0)
shell_dir=${cur_dir/\./$(pwd)}

############  example ##################
#distinct_server.sh -DdataDir=/data/bloomfilter_db -Dport=
########################################
params=$@

cmd="java  -cp ${shell_dir}/../target/distinct-server.jar:$ANA_HOME $params com.github.distinct_server.Server" 
echo '-----------------------------------------------'
echo $cmd
echo '-----------------------------------------------'

$cmd 


