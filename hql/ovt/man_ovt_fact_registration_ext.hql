use ovt;
DROP TABLE IF EXISTS man_ovt_fact_registration_ext;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_fact_registration_ext
(
  `reg_key` int,
  `reg_dt_key` int,
  `ad_make_desc` string,
  `ad_model_desc` string,
  `ad_trim_desc` string,
  `ad_body_desc` string,
  `ad_transmission_desc` string,
  `ad_engine_desc` string,
  `ad_interior_color_desc` string,
  `ad_exterior_color_desc` string,
  `ad_veh_type` string,
  `dw_created_on` timestamp,
  `dw_updated_on` timestamp,
  `insert_batch_key` int,
  `update_batch_key` int,
  `pickup_addr_line1` string,
  `pickup_addr_line2` string,
  `pickup_city` string,
  `pickup_state_cd_prov` string,
  `pickup_cntry` string,
  `pickup_zip` string,
  `ad_veh_subtype` string,
  `ad_engine_size_units` string,
  `ad_engine_fuel_type` string,
  `ad_drivetrain` string,
  `ad_user_ext_color_desc` string,
  `ad_user_int_color_desc` string,
  `ad_door_count` string,
  `ad_roof_desc` string,
  `ad_audio_desc` string,
  `ad_ignition_type` string,
  `ad_power_window_type` string,
  `ad_fuel_intake_type` string,
  `ad_engine_cooling_type` string,
  `ad_user_trim_desc` string,
  `ad_interior_type` string,
  `ad_year` int,
  `green_light` string,
  `yellow_light` string,
  `blue_light` string,
  `red_light` string,
  `country_of_origin` string,
  `ad_air_cond_desc` string,
  `ad_air_bag_desc` string,
  `ad_cruise_control_desc` string,
  `ad_elec_lock_desc` string,
  `ad_elec_seat_desc` string,
  `jd_powers_cat_desc` string,
  `jd_powers_sub_cat_desc` string,
  `ad_power_braking_desc` string,
  `ad_power_steering_desc` string,
  `ad_rear_defrost_desc` string,
  `ad_side_air_bag_desc` string,
  `ad_tilt_steering_desc` string,
  `ad_wheel_base_desc` string,
  `cr_est_labor_amt` int,
  `cr_est_parts_amt` int,
  `cert_flg` int,
  `ford_cat_cd` string,
  `hist_bus_unit` string,
  `nada_value` int,
  `psi_flg` int,
  `sales_type` string,
  `salv_title_status` string,
  `title_status_cd` string,
  `title_num` string,
  `auct_to_buyer_miles` int,
  `auct_to_seller_miles` int,
  `seller_num_on_check` int,
  `special_sale_ind` string,
  `print_physical_check` string,
  `signer_num` int,
  `auction_invc_num` int,
  `orig_cr_type` string,
  `final_cr_type` string,
  `manheim_id` string,
  `reject_reason_cd` string,
  `company_cd` string,
  `drivable` string,
  `pickup_contact_phone` string,
  `ecr_link` string,
  `damage_key` int,
  `title_status_desc` string,
  `title_type_cd` string,
  `title_type_desc` string,
  `vdp_link` string,
  `default_img_url` string,
  `starting_bid_amt` int,
  `sign_off_cr_dt_key` int,
  `days_to_pickup` int,
  `legacy_sale_grp` string,
  `black_book_value` int,
  `ingestion_timestamp` string)
PARTITIONED BY (
  `theyear` smallint,
  `themonth` smallint,
  `theday` smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_fact_registration_ext';
