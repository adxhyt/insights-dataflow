#!/bin/bash

FILEPATH=$1
NAMENODE=$2
FILE=${FILEPATH%.*}
unzip $FILEPATH
tail -n +2 $FILE.txt > $FILE.tmp && mv $FILE.tmp $FILE.txt

hdfs dfs -rm /data/database/experian/originations/new/*
hdfs dfs -put $FILE'.txt' /data/database/experian/originations/new/

rm $FILE.txt

if [ -z "$NAMENODE" ]
then
  echo "Cant find namenode add as second argument to run beeline"
  exit 1
fi

/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/experian/originations.hql