#!/bin/bash 

db=$1 
hive_port=13001


NAMENODE_HOST='dev-na-lxhdn01.cloudapp.net'
EDGENODE_HOST='dev-na-LXHDE01'
SQL_SERVER_HOST='dev-na-LXHDE01'
MYSQL_HOST='dev-na-LXHDE01'
JDBC_URL='jdbc:mysql://dev-na-LXHDE01/rpm'
SQL_USER='insights'
SQL_PASS='M@ins3a'

current_time=`date +"%Y-%m-%dT%H:%MZ"`
echo $current_time

echo 'hive port:'$hive_port
current_sharelib=`sudo su -m -l oozie -c"oozie admin -sharelibupdate" | grep 'sharelibDirNew' | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'`
echo 'current sharelib '$current_sharelib
sudo su -m -l oozie -c "oozie job -config /home/oozie/insights-dataflow/oozie/configs/standalone_jobs/incremental.properties -run -Ddb_name=$db -Dcurrent_sharelib=$current_sharelib -Dnamenode_host=$NAMENODE_HOST -Dedgenode_host=$EDGENODE_HOST -Dsql_user=$SQL_USER -Dsql_pass=$SQL_PASS -Dnominal_time=$current_time -Dhive_port=$hive_port" 
