#!/bin/bash

NAMENODE=$1

hadoop fs -mkdir -p /data/database/
hadoop fs -mkdir -p /hql/ovt/avro/
hadoop fs -copyFromLocal -f hql/ovt/avro/* /user/oozie/share/hql/avro/
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database insights;'
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database rpm;'
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database vdm;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database mmr;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database psa;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database manheim;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database vauto;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database vauto_new;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database experian;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database at;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database ovt;';
/usr/hdp/current/spark-client/bin/beeline -u jdbc:hive2://$NAMENODE:13001/ -e 'create database chrome;';
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
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vauto_new/vauto_market_pricing.hiveql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vauto_new/vauto_recent_market_data.hiveql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/vauto_new/vauto_sold_market_vehicle.hiveql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/experian/originations.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/at/geo.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/at/at_nz_inventory_daily_snp_ext.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/  --hiveconf namenode=$NAMENODE -f  hql/ovt/man_ovt_dim_customer.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/  --hiveconf namenode=$NAMENODE -f  hql/ovt/man_ovt_dim_flndr.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/  --hiveconf namenode=$NAMENODE -f  hql/ovt/man_ovt_dim_make_model_trim.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/  --hiveconf namenode=$NAMENODE -f  hql/ovt/man_ovt_dim_veh_att_list2.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/  --hiveconf namenode=$NAMENODE -f  hql/ovt/man_ovt_dim_veh_att_list.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/ovt/man_ovt_fact_registration_ext.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/ovt/man_ovt_fact_registration.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_body_type.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_category_definition.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_division_definition.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_engine.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_model_definition.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_style.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_subdivision_definition.hql
/usr/hdp/current/spark-client/bin/beeline -u  jdbc:hive2://$NAMENODE:13001/ -f hql/chrome/chrome_vehicle_description.hql

./scripts/add_partitions.sh -h $NAMENODE -d vauto -t vauto_recent_market_data -l /data/database/vauto/vauto_recent_market_data
./scripts/add_partitions.sh -h $NAMENODE -d vauto -t vauto_market_pricing -l /data/database/vauto/vauto_market_pricing
./scripts/add_partitions.sh -h $NAMENODE -d vauto -t vauto_sold_market_vehicle -l /data/database/vauto/vauto_sold_market_vehicle
./scripts/add_partitions.sh -h $NAMENODE -d vauto_new -t vauto_recent_market_data -l /data/database/vauto/vauto_market_pricing_new
./scripts/add_partitions.sh -h $NAMENODE -d vauto_new -t vauto_market_pricing -l /data/database/vauto/vauto_recent_market_data_new
./scripts/add_partitions.sh -h $NAMENODE -d vauto_new -t vauto_sold_market_vehicle -l /data/database/vauto/vauto_sold_market_vehicle_new
./scripts/add_partitions.sh -h $NAMENODE -d ovt -t man_ovt_fact_registration -l /data/database/manheim/man_ovt_raw/man_ovt_fact_registration
./scripts/add_partitions.sh -h $NAMENODE -d ovt -t man_ovt_fact_inventory -l /data/database/manheim/man_ovt_raw/man_ovt_fact_inventory
./scripts/add_partitions.sh -h $NAMENODE -d ovt -t man_ovt_fact_registration_ext -l /data/database/manheim/man_ovt_raw/man_ovt_fact_registration_ext
./scripts/add_partitions.sh -h $NAMENODE -d at  -t at_nz_inventory_daily_snp_ext  -l /data/database/at/nz_inv_dly_detail_snapshot

