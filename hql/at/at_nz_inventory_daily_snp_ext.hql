use at;
drop  table if exists at_nz_inventory_daily_snp_ext;
CREATE EXTERNAL TABLE  at_nz_inventory_daily_snp_ext (
	   effective_date  STRING,
	   listing_skey  INT,
	   car_durable_key  INT,
	   car_id  INT,
	   vehicle_skey  INT,
	   customer_id  INT,
	   current_record_ind  INT,
	   price_num  DOUBLE,
	   msrp_num  DOUBLE,
	   mileage_num  INT,
	   zip  STRING,
	   new_ind  INT,
	   certified_ind  INT,
	   make_name  STRING,
	   model_name  STRING,
	   model_year_name  INT,
	   trim_name  STRING,
	   bodystyle_name  STRING,
	   vin  STRING,
	   engine_type_name  STRING,
	   drive_type_name  STRING,
	   transmission_name  STRING,
	   transmission_type  STRING,
	   make_id  STRING,
	   model_id  STRING)
	COMMENT ' - This table has data exported from pub.nz_inventory_daily_snp_ext.'
	PARTITIONED BY (
	   year  smallint,
	   month smallint,
	   day smallint)
	ROW FORMAT DELIMITED
	  FIELDS TERMINATED BY ','
	  LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION
	  '/data/database/at/nz_inv_dly_detail_snapshot';
