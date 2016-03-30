#!/bin/bash
FILE=$1
unzip $FILE'.zip'
tail -n +2 $FILE'.txt' > $FILE.tmp && mv $FILE.tmp $FILE.'txt'

hdfs dfs -rm /data/database/experian/originations/new/*
hdfs dfs -put $FILE'.txt' /data/database/experian/originations/new/