use ovt;
DROP TABLE IF EXISTS man_ovt_fact_inventory;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_fact_inventory(
`inventory_key` int,
`auction_key` int,
`auction_cd` string,
`distribution_center_key` int,
`inventory_item_id` int,
`source_system_key` int,
`originating_source_system_key` int,
`seller_srvc_ordr_id` int,
`sblu` int,
`vin` string,
`model_yr` int,
`make` string,
`model` string,
`trim` string,
`body` string,
`make_model_trim_key` int,
`work_order_number` int,
`owner_cust_key` int,
`check_in_dt_key` int,
`check_in_ts` timestamp,
`check_out_dt_key` int,
`check_out_ts` timestamp,
`days_on_lot` int,
`active_inventory_flg` int,
`dw_created_on` timestamp,
`dw_updated_on` timestamp,
`insert_batch_key` int,
`update_batch_key` int,
`vehicle_mileage_num` int,
`vehicle_hrs_num` int,
`vehicle_km_num` int,
`checked_out_by` string,
`engine_type` string,
`reg_key` int,
`manheim_id` string,
`certification_flg` int,
`legacy_business_unit_cd` string,
`title_flg` int,
`flndr_key` int,
`sold_ts` timestamp,
`chkin_to_sold_days` int,
`consigned_dt_key` int,
`country_of_origin` string,
`man_stock_num` string,
`transportation_type` string,
`transporter_nm` string,
`act_cash_value` int,
`reg_cr_grade_ts` timestamp,
`offrng_cr_grade_ts` timestamp,
`chkin_to_reg_cr_days` int,
`chkin_to_offrng_cr_days` int,
`recvd_to_consigned_days` int,
`seller_lease_account` string,
`days_to_condition_report` int,
`days_to_title` int,
`source_system_user_id` string,
`title_received_ts` timestamp,
`title_status` string,
`owner_uid` string,
`seller_uid` string,
`seller_cust_key` int,
`times_offered` int,
`inventory_age` int,
`drivable` string,
`inventory_status` string,
`vehicle_unk_uom_num` int,
`owner_acct_key` int,
`seller_acct_key` int,
`sign_off_cr_dt_key` int,
`init_cr_grade_ts` timestamp,
`final_cr_grade_ts` timestamp,
`init_cr_grade` int,
`final_cr_grade` int,
`chckin_to_init_cr_days` int,
`chckin_to_final_cr_days` int,
`redeemed_ts` timestamp,
`redeemed_dt_key` int,
`ingestion_timestamp` timestamp
)
COMMENT 'This table is imported from Manheim Oracle table fact_inventory '
PARTITIONED BY (
theyear smallint,
themonth smallint,
theday smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_fact_registration';



