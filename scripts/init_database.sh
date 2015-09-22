#!/bin/bash 

NAMENODE=$1

hadoop fs -mkdir -p /data/database/
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database insights;'
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database rpm;'
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database vdm;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database mmr;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database psa;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database manheim;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database vauto;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database experian;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database at;';
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/schemas/sales.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/schemas/inventory.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/manheim/manheim.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vdm/options.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vdm/packages.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vdm/vehicles.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/mmr/sales.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vauto/vauto_market_pricing.hiveql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vauto/vauto_recent_market_data.hiveql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vauto/vauto_sold_market_vehicle.hiveql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/experian/originations.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/at/geo.hql


