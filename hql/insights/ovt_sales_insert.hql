use insights;
set mapreduce.input.fileinputformat.split.maxsize=34396550;
set  hive.auto.convert.join=false;
SET spark.sql.shuffle.partitions=24;
INSERT OVERWRITE TABLE insights.sales_report_cached SELECT
coalesce(vdmv.vb_make, mmr.mmr_make) as make,
 vdmv.vb_make as makeref,
 'n/a' as registration,
 'n/a' as chassis,
 vdmv.ev_trim as derivative,
abs(hash(vdmv.ev_trim)) as derivativeid,
'n/a' as registrationdate,
 vdmv.vb_ext_color_generic_descr as exteriorcolour,
unix_timestamp(ovt_reg.reg_ts)  as creationdate,
0 as salechannelid,
'n/a' as salechannel,
seller_cust.cur_cust_nm as vendortradingname,
seller_cust.country_cd as vendorcountryid,
seller_cust.country_cd as vendorcountryname,
seller_cust.cust_id as vendorid,
0 as vehicleageindays,
date(ovt_reg.sold_ts) as solddate,
unix_timestamp(ovt_reg.sold_ts) as solddatets ,
'n/a' as vatqualified,
ovt_reg.pur_amt as sold_price,
ovt_reg.buyer_fees_amt as buyerpremium,
0 as delivery,
buyer_cust.cur_cust_nm as buyer,
buyer_cust.cust_id as buyerid, 
buyer_cust.cur_cust_nm as buyercode,
0 as deliverylocation,
case when ovt_reg.sold_ts is NULL  then 'Y' else 'N' end as activesale,
coalesce(vdmv.vb_model,  mmr.mmr_model) as model,
'n/a' as code,
coalesce(cast(vdmv.vb_model_year as int), mmr.mmr_model_year) as modelyear,
vdmv.ev_model_id as model_code,
ovt_reg.vehicle_mileage_cnt,
ovt_reg.ireg_to_sale_days  as daysonsale,
ovt_reg.cur_floor_price  as auctionprice,
 vdmv.ev_transmission as transmission,
abs(hash(vdmv.ev_transmission)) as transmissionid,
NULL as stockage,
vdmv.vb_vehicle_type as vehicle_type,
'none' as salessession,
0 as salesessionid, 
'none' as tacticname, 
0 as tacticid,
'Manheim' as commercialconceptname,
1 as countryid,
auction.country_nm as countryname,  
0 as totaldamagesnetprice,
vdmv.ev_engine_fuel_type_descr as fueltype,
ovt_reg.uniq_reg_id as vehicleid, 
ovt_reg.vin,
0 as sourceid,
'n/a' as sourcename, 
2  as buyertypeid,
'Dealer Purchase' as buyertypename,
'Dealer Purchase' as buyertypedesc,
gross_pur_amt as priceexcludingvat,
NULL  as directsaleid,
NULL as sellername,
NULL as sellerid,
year(ovt_reg.sold_ts) as soldyear,
month(ovt_reg.sold_ts) as soldmonth,
 '' as ageinweeksbandname,
 0 as ageinweeksbandid,
 NULL as stockageWeeksBandName,
 NULL as stockageWeeksBandId,
NULL as damagesBandName,
NULL as damagesBandId,
     case WHEN ovt_reg.vehicle_mileage_cnt >=0 AND ovt_reg.vehicle_mileage_cnt <10000 THEN '0-10 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=10000 AND ovt_reg.vehicle_mileage_cnt <20000 THEN '10 001-20 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=20000 AND ovt_reg.vehicle_mileage_cnt <30000 THEN '20 001-30 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=30000 AND ovt_reg.vehicle_mileage_cnt <40000 THEN '30 001-40 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=40000 AND ovt_reg.vehicle_mileage_cnt <50000 THEN '40 001-50 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=50000 AND ovt_reg.vehicle_mileage_cnt <75000 THEN '50 001-75 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=75000 AND ovt_reg.vehicle_mileage_cnt <100000 THEN '75001-100 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=100000 AND ovt_reg.vehicle_mileage_cnt <150000 THEN '100 001-150 000'
          WHEN ovt_reg.vehicle_mileage_cnt >=150000 AND ovt_reg.vehicle_mileage_cnt <999999 THEN 'over 150 000'
      end mileageBandName,
    case
            WHEN ovt_reg.vehicle_mileage_cnt >=0 AND ovt_reg.vehicle_mileage_cnt <10000 THEN 0
            WHEN ovt_reg.vehicle_mileage_cnt >=10000 AND ovt_reg.vehicle_mileage_cnt <20000 THEN 1
            WHEN ovt_reg.vehicle_mileage_cnt >=20000 AND ovt_reg.vehicle_mileage_cnt <30000 THEN 2
            WHEN ovt_reg.vehicle_mileage_cnt >=30000 AND ovt_reg.vehicle_mileage_cnt <40000 THEN 3
            WHEN ovt_reg.vehicle_mileage_cnt >=40000 AND ovt_reg.vehicle_mileage_cnt <50000 THEN 4
            WHEN ovt_reg.vehicle_mileage_cnt >=50000 AND ovt_reg.vehicle_mileage_cnt <75000 THEN 5
            WHEN ovt_reg.vehicle_mileage_cnt >=75000 AND ovt_reg.vehicle_mileage_cnt <100000 THEN 6
            WHEN ovt_reg.vehicle_mileage_cnt >=100000 AND ovt_reg.vehicle_mileage_cnt <150000 THEN 7
            WHEN ovt_reg.vehicle_mileage_cnt >=150000 AND ovt_reg.vehicle_mileage_cnt <999999 THEN 8
end mileageBandId,
NULL stockageweeks,
NULL vehicleageweeks,
buyer_cust.country_cd as buyercountry,
buyer_cust.country_cd as buyercountryid,
'n/a' as vendortown,
0 as locationid,
1 as commercialconcepttypeid,
1 as pl_id,
vdmo.vdm_options_desc_map,
vdmo.vdm_options_group_map,
vdmv.b_vehicle_id as vdm_b_vehicle_id,
vdmv.vb_vin as vdm_vb_vin,
vdmv.vb_associate_vin as vdm_vb_associate_vin,
vdmv.vb_vehicle_type as vdm_vb_vehicle_type,
vdmv.vb_model_year as vdm_vb_model_year,
vdmv.vb_make as vdm_vb_make,
vdmv.vb_model as vdm_vb_model,
vdmv.ev_trim as vdm_ev_trim,
vdmv.ev_manufacturer_trim as vdm_ev_manufacturer_trim,
vdmv.ev_style_id as vdm_ev_style_id,
vdmv.ev_manufacturer_style_id as vdm_ev_manufacturer_style_id,
vdmv.vb_subdivision_name as vdm_vb_subdivision_name,
vdmv.ev_msrp as vdm_ev_msrp,
vdmv.vb_half_year_ind as vdm_vb_half_year_ind,
vdmv.vb_manufacturer as vdm_vb_manufacturer,
vdmv.vb_number_of_wheels as vdm_vb_number_of_wheels,
vdmv.vb_gvwr as vdm_vb_gvwr,
vdmv.vb_language_code as vdm_vb_language_code,
vdmv.vb_source_code as vdm_vb_source_code,
vdmv.vb_country_of_origin_code as vdm_vb_country_of_origin_code,
vdmv.vb_country_code as vdm_vb_country_code,
vdmv.vb_ext_color_generic_descr as vdm_vb_ext_color_generic_descr,
vdmv.vb_ext_color_mfr_rgb_code as vdm_vb_ext_color_mfr_rgb_code,
vdmv.vb_ext_color_mfr_code as vdm_vb_ext_color_mfr_code,
vdmv.vb_ext_color_mfr_description as vdm_vb_ext_color_mfr_description,
vdmv.vb_ext_color_generic_descr2 as vdm_vb_ext_color_generic_descr2,
vdmv.vb_ext_color_mfr_rgb_code_2 as vdm_vb_ext_color_mfr_rgb_code_2,
vdmv.vb_ext_color_mfr_code_2 as vdm_vb_ext_color_mfr_code_2,
vdmv.vb_ext_color_mfr_description_2 as vdm_vb_ext_color_mfr_description_2,
vdmv.vb_int_color_mfr_code as vdm_vb_int_color_mfr_code,
vdmv.vb_int_color_mfr_description as vdm_vb_int_color_mfr_description,
vdmv.ev_region_code as vdm_ev_region_code,
vdmv.ev_market_class_id as vdm_ev_market_class_id,
vdmv.ev_model_id as vdm_ev_model_id,
vdmv.ev_transmission as vdm_ev_transmission,
vdmv.ev_drivetrain as vdm_ev_drivetrain,
vdmv.ev_wheelbase as vdm_ev_wheelbase,
vdmv.ev_market_class_description as vdm_ev_market_class_description,
vdmv.ev_number_of_doors as vdm_ev_number_of_doors,
vdmv.ev_passenger_capacity as vdm_ev_passenger_capacity,
vdmv.ev_fuel_economy_city_ville as vdm_ev_fuel_economy_city_ville,
vdmv.ev_fuel_economy_highway_route as vdm_ev_fuel_economy_highway_route,
vdmv.ev_fuel_eco_units_of_measure as vdm_ev_fuel_eco_units_of_measure,
vdmv.ev_style_name_without_trim as vdm_ev_style_name_without_trim,
vdmv.ev_body_type_primary as vdm_ev_body_type_primary,
vdmv.ev_body_type_secondary as vdm_ev_body_type_secondary,
vdmv.ev_odometer_reading as vdm_ev_odometer_reading,
vdmv.ev_odometer_reading_capture_ts as vdm_ev_odometer_reading_capture_ts,
vdmv.ev_odometer_digits as vdm_ev_odometer_digits,
vdmv.ev_odometer_units_of_measure as vdm_ev_odometer_units_of_measure,
vdmv.ev_odometer_status as vdm_ev_odometer_status,
vdmv.ev_odometer_type as vdm_ev_odometer_type,
vdmv.ev_engine_type as vdm_ev_engine_type,
vdmv.ev_engine_displacement as vdm_ev_engine_displacement,
vdmv.ev_engine_induction as vdm_ev_engine_induction,
vdmv.ev_engine_fuel_type_descr as vdm_ev_engine_fuel_type_descr,
vdmv.ev_engine_horse_power as vdm_ev_engine_horse_power,
vdmv.ev_vehicle_sub_type as vdm_ev_vehicle_sub_type,
vdmv.ev_line_name as vdm_ev_line_name,
vdmv.ev_ag_series_code as vdm_ev_ag_series_code,
vdmv.ev_in_service_date as vdm_ev_in_service_date,
vdmv.ev_mid as vdm_ev_mid,
vdmv.ev_invoice_wholesale as vdm_ev_invoice_wholesale,
vdmv.vb_created_timestamp as vdm_vb_created_timestamp,
vdmv.vb_created_by as vdm_vb_created_by,
vdmv.vb_last_update_timestamp as vdm_vb_last_update_timestamp,
vdmv.vb_last_update_by as vdm_vb_last_update_by,
mmr.mmr_body as mmr_body,
mmr.mmr_edition as mmr_edition,
mmr.mmr_algorithm as mmr_algorithm,
mmr.mmr_national_value as mmr_nationalvalue,
mmr.mmr_national_sample_size as mmr_nationalsamplesize,
NULL  as rpm_lease_start_date,
NULL as rpm_lease_start_year,
NULL  as rpm_lease_start_month,
NULL  as rpm_lease_start_day,
NULL as rpm_lease_end_date,
NULL  as rpm_lease_end_year,
NULL  as rpm_lease_end_month,
NULL  as rpm_lease_end_day,
NULL  as  rpm_lease_start_ts,
NULL  as rpm_lease_end_ts,
NULL as polk_corporation,
NULL as polk_report_year_month,
NULL as polk_transaction_date,
NULL as polk_transaction_ts,
NULL as polk_trans_price,
NULL as polk_data_type,
NULL as polk_origin,
NULL as polk_purchase_lease,
NULL as polk_vehicle_count,
NULL as polk_dealer_name,
NULL as polk_dealer_address,
NULL as polk_dealer_town,
NULL as polk_dealer_state,
NULL as polk_dealer_zip,
NULL as polk_dealer_dma,
NULL as polk_fran_ind,
NULL as rpm_vehicle_address,
NULL as rpm_vehicle_city,
NULL as rpm_vehicle_state,
NULL as rpm_vehicle_zip,
GEO1.dma_durable_key as geo_dma_durable_key,
GEO1.dma_code as geo_dma_code,
GEO1.dma_desc as geo_dma_desc,
GEO1.city as geo_city,
GEO1.state_code as geo_state_code,
GEO1.county as geo_county,
GEO1.country_code as geo_country_code,
GEO1.latitude as latitude,
GEO1.longitude as geo_longitude,
GEO1.submarket as geo_submarket,
GEO1.tim_zone_desc as geo_tim_zone_desc,
GEO1.dma_id as geo_dma_id,
cast(ovt_reg.pur_amt as int) as pur_amt,
cast(ovt_reg.at_sale_nat_mmr as int) as at_sale_nat_mmr,
ovt_reg.gross_txn_flg as gross_txn_flg,
ovt_reg.offrng_flg as offrng_flg,
ovt_reg.offered_cnt as offered_cnt,
 unix_timestamp(check_in_ts),
 unix_timestamp(preview_ts),
 unix_timestamp(act_offrng_start_ts),
 unix_timestamp(act_offrng_end_ts),
ovt_reg.auction_key as auction_key,
ovt_reg.pur_type_key as pur_type_key,
ovt_reg.at_reg_nat_mmr as at_reg_nat_mmr,
ovt_reg.at_reg_rgn_mmr as at_reg_rgn_mmr,
ovt_reg.gross_pur_amt as  gross_pur_amt,
ovt_reg.buyer_fees_amt as buyer_fees_amt,
ovt_reg.net_seller_amt as net_seller_amt,
ovt_reg.arbitrated_amt as arbitrated_amt,
ovt_reg.reg_cr_grade_ts as reg_cr_grade_ts,
ovt_reg.offrng_cr_grade as offrng_cr_grade,
ovt_reg.mmr_mileage_adj_amt as mmr_mileage_adj_amt,
ovt_reg.ireg_to_sale_days as ireg_to_sale_days,
ovt_reg.ireg_to_ioffrng_days as ireg_to_ioffrng_days,
ovt_reg.ireg_to_ipreview_days as ireg_to_ipreview_days,
ovt_reg.icheck_in_to_sale_days as icheck_in_to_sale_days,
ovt_reg.icheck_in_to_ioffrng_days as icheck_in_to_ioffrng_days,
ovt_reg.icheck_in_to_ipreview_days as icheck_in_to_ipreview_days,
ovt_reg.auction_format as auction_format,
ovt_reg.seller_fees_amt as seller_fees_amt,
ovt_reg.seller_tax_amt as seller_tax_amt,
ovt_reg.seller_adjust_amt as seller_adjust_amt,
ovt_reg.buyer_tax_amt as buyer_tax_amt,
ovt_reg.buyer_adjust_amt as buyer_adjust_amt,
ovt_reg.reg_cr_grade as reg_cr_grade
from 

 ovt.man_ovt_fact_registration ovt_reg 
join  ovt.man_ovt_dim_customer buyer_cust on ovt_reg.buyer_cust_key = buyer_cust.cust_key
join ovt.man_ovt_dim_customer seller_cust on  ovt_reg.seller_cust_key = seller_cust.cust_key
join ovt.man_ovt_dim_auction auction on auction.auction_key = ovt_reg.auction_key
left join at.geo GEO1 on GEO1.zip_code=Substring(auction.zip_cd, 1, 5) 
left join mmr.sales mmr on ovt_reg.vin = mmr.m_vin
left join vdm.vehicles vdmv on vdmv.vb_vin=ovt_reg.vin 
left join vdm.vdm_options_packages vdmo on ovt_reg.vin = vdmo.vin;
SET spark.sql.shuffle.partitions=1;
