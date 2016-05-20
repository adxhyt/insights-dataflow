#!/bin/bash 

db=$1 
frequency=$2

NAMENODE_HOST='dev-na-lxhdn01.cloudapp.net'
EDGENODE_HOST='dev-na-LXHDE01'
SQL_SERVER_HOST='dev-na-LXHDE01'
MYSQL_HOST='dev-na-LXHDE01'
JDBC_URL='jdbc:mysql://dev-na-LXHDE01/rpm'
SQL_USER='insights'
SQL_PASS='M@ins3a'

current_time=`date +"%Y-%m-%dT%H:%MZ"`
echo $current_time

current_sharelib=`sudo su -m -l oozie -c"oozie admin -sharelibupdate" | grep 'sharelibDirNew' | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'`
echo 'current sharelib '$current_sharelib
sudo su -m -l oozie -c "oozie job -config /home/oozie/insights-dataflow/oozie/configs/coordinators/coordinator_incremental.properties -run -Ddb_name=$db -Dcurrent_sharelib=$current_sharelib -Dnamenode_host=$NAMENODE_HOST -Dedgenode_host=$EDGENODE_HOST -Dsql_user=$SQL_USER -Dsql_pass=$SQL_PASS -Dstart_time=$current_time -Dfrequency=$2"
