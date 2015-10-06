use insights;
add jar ${hivevar:NameNode}/user/oozie/share/lib/adaltas-hive-udf-0.0.1-SNAPSHOT.jar;
DROP FUNCTION IF EXISTS to_map;
CREATE FUNCTION to_map as "com.adaltas.UDAFToMap";
SET spark.sql.shuffle.partitions=6;
INSERT OVERWRITE TABLE insights.sales_report_cached_stg SELECT
coalesce(v.make, mmr.mmr_make) as make,
 v.make as makeref,
 'n/a' as registration,
 'n/a' as chassis,
 v.trim_level as derivative,
abs(hash(v.trim_level)) as derivativeid,
'n/a' as registrationdate,
 V.color as exteriorcolour,
P.created_at as creationdate,
0 as salechannelid,
'n/a' as salechannel,
 D.name as vendortradingname,
0 as vendorcountryid,
'United States' as vendorcountryname,
 D.id as vendorid,
0 as vehicleageindays,
P.date_of_purchase as solddate,
unix_timestamp(P.updated_at) as solddatets ,
'n/a' as vatqualified,
P.purchase_price as sold_price,
0 as buyerpremium,
0 as delivery,
'n/a' as buyer,
0 as buyerid, 
'n/a' as buyercode,
0 as deliverylocation,
case when V.status='Sold' or V.status='Sold Upstream' then 'Y' else 'N' end as activesale,
coalesce(vdmv.vb_model, V.model, mmr.mmr_model) as model,
'n/a' as code,
coalesce(cast(V.model_year as int), mmr.mmr_model_year) as modelyear,
 V.model_serial_number as model_code,
G.mileage,
datediff(P.created_at, G.created_at) as daysonsale,
0 as auctionprice,
 V.transmission_type as transmission,
abs(hash(V.transmission_type)) as transmissionid,
 G.stockage,
'Car' as vehicle_type,
'none' as salessession,
0 as salesessionid, 
'none' as tacticname, 
0 as tacticid,
'none' as commercialconceptname,
1 as countryid,
'United States' as countryname,  
 AD.repair_cost as totaldamagesnetprice,
 AV.fuel_type as fueltype,
V.id as vehicleid, 
 V.vin,
0 as sourceid,
'n/a' as sourcename, 
case when P.purchase_type='Lessee Purchase' then 1 when P.purchase_type='Dealer Purchase' then 2 else 0 end as buyertypeid,
P.purchase_type as buyertypename,
P.purchase_type as buyertypedesc,
P.purchase_price as priceexcludingvat,
P.id as directsaleid,
'n/a' as sellername,
0 as sellerid,
year(P.date_of_purchase) as soldyear,
month(P.date_of_purchase) as soldmonth,
 '' as ageinweeksbandname,
 0 as ageinweeksbandid,
     CASE WHEN G.stockage/7 >=0 AND G.stockage/7 <=7 THEN 'under 1 '
            WHEN G.stockage/7 >=8 AND G.stockage/7 <=14 THEN '1 - 2'
            WHEN G.stockage/7 >=15 AND G.stockage/7 <=21 THEN '2 - 3'
            WHEN G.stockage/7 >=22 AND G.stockage/7 <=28 THEN '3 - 4'
            WHEN G.stockage/7 >=29 AND G.stockage/7 <=35 THEN '4 - 5'
            WHEN G.stockage/7 >=36 AND G.stockage/7 <=42 THEN '5 - 6'
            WHEN G.stockage/7 >=43 AND G.stockage/7 <=49 THEN '6 - 7'
            WHEN G.stockage/7 >=50 AND G.stockage/7 <=56 THEN '7 - 8'
            WHEN G.stockage/7 >=57 AND G.stockage/7 <=63 THEN '8 - 9'
            WHEN G.stockage/7 >=64 AND G.stockage/7 <=70 THEN '9 - 10'
            WHEN G.stockage/7 >=71 AND G.stockage/7 <=77 THEN '10 - 11'
            WHEN G.stockage/7 >=78 AND G.stockage/7 <=84 THEN '11 - 12'
            WHEN G.stockage/7 >=85 AND G.stockage/7 <=91 THEN '12 - 13'
            WHEN G.stockage/7 >=92 AND G.stockage/7 <=98 THEN '13 - 14'
            WHEN G.stockage/7 >=99 AND G.stockage/7 <=105 THEN '14 - 15'
            WHEN G.stockage/7 >=106 AND G.stockage/7 <=112 THEN '15 - 16'
            WHEN G.stockage/7 >=113 AND G.stockage/7 <=100000 THEN 'over 16'
      end as stockageWeeksBandName,
      CASE  WHEN G.stockage/7 >=0 AND G.stockage/7 <=7 THEN 0
            WHEN G.stockage/7 >=8 AND G.stockage/7 <=14 THEN 1
            WHEN G.stockage/7 >=15 AND G.stockage/7 <=21 THEN 2
            WHEN G.stockage/7 >=22 AND G.stockage/7 <=28 THEN 3
            WHEN G.stockage/7 >=29 AND G.stockage/7 <=35 THEN 4
            WHEN G.stockage/7 >=36 AND G.stockage/7 <=42 THEN 5
            WHEN G.stockage/7 >=43 AND G.stockage/7 <=49 THEN 6
            WHEN G.stockage/7 >=50 AND G.stockage/7 <=56 THEN 7
            WHEN G.stockage/7 >=57 AND G.stockage/7 <=63 THEN 8
            WHEN G.stockage/7 >=64 AND G.stockage/7 <=70 THEN 9
            WHEN G.stockage/7 >=71 AND G.stockage/7 <=77 THEN 10
            WHEN G.stockage/7 >=78 AND G.stockage/7 <=84 THEN 11
            WHEN G.stockage/7 >=85 AND G.stockage/7 <=91 THEN 12
            WHEN G.stockage/7 >=92 AND G.stockage/7 <=98 THEN 13
            WHEN G.stockage/7 >=99 AND G.stockage/7 <=105 THEN 14
            WHEN G.stockage/7 >=106 AND G.stockage/7 <=112 THEN 15
            WHEN G.stockage/7 >=113 AND G.stockage/7 <=100000 THEN 16
      end as stockageWeeksBandId,
      CASE WHEN AD.repair_cost >=0 AND AD.repair_cost <99 THEN 'under 100'
           WHEN AD.repair_cost >=100 AND AD.repair_cost <500 THEN '100 - 500'
           WHEN AD.repair_cost >=500 AND AD.repair_cost <750 THEN '501 - 750'
           WHEN AD.repair_cost >=750 AND AD.repair_cost <100000 THEN 'over 750'
      end as damagesBandName,
      case WHEN AD.repair_cost >=0 AND AD.repair_cost <100 THEN 0
           WHEN AD.repair_cost >=100 AND AD.repair_cost <500 THEN 1
           WHEN AD.repair_cost >=500 AND AD.repair_cost <750 THEN 2
           WHEN AD.repair_cost >=750 AND AD.repair_cost <100000 THEN 3
      end as damagesBandId,
     case WHEN G.mileage >=0 AND G.mileage <10000 THEN '0-10 000'
          WHEN G.mileage >=10000 AND G.mileage <20000 THEN '10 001-20 000'
          WHEN G.mileage >=20000 AND G.mileage <30000 THEN '20 001-30 000'
          WHEN G.mileage >=30000 AND G.mileage <40000 THEN '30 001-40 000'
          WHEN G.mileage >=40000 AND G.mileage <50000 THEN '40 001-50 000'
          WHEN G.mileage >=50000 AND G.mileage <75000 THEN '50 001-75 000'
          WHEN G.mileage >=75000 AND G.mileage <100000 THEN '75001-100 000'
          WHEN G.mileage >=100000 AND G.mileage <150000 THEN '100 001-150 000'
          WHEN G.mileage >=150000 AND G.mileage <999999 THEN 'over 150 000'
      end mileageBandName,
    case
            WHEN G.mileage >=0 AND G.mileage <10000 THEN 0
            WHEN G.mileage >=10000 AND G.mileage <20000 THEN 1
            WHEN G.mileage >=20000 AND G.mileage <30000 THEN 2
            WHEN G.mileage >=30000 AND G.mileage <40000 THEN 3
            WHEN G.mileage >=40000 AND G.mileage <50000 THEN 4
            WHEN G.mileage >=50000 AND G.mileage <75000 THEN 5
            WHEN G.mileage >=75000 AND G.mileage <100000 THEN 6
            WHEN G.mileage >=100000 AND G.mileage <150000 THEN 7
            WHEN G.mileage >=150000 AND G.mileage <999999 THEN 8
end mileageBandId,
G.stockage/7 as stockageweeks,
0 as vehicleageweeks,
'n/a' as buyercountry,
0 as buyercountryid,
'n/a' as vendortown,
0 as locationid,
0 as commercialconcepttypeid,
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
V.lease_start_date as rpm_lease_start_date,
year(V.lease_start_date) as rpm_lease_start_year,
month(V.lease_start_date) as rpm_lease_start_month,
day(V.lease_start_date) as rpm_lease_start_day,
V.lease_end_date as rpm_lease_end_date,
year(V.lease_end_date) as rpm_lease_end_year,
month(V.lease_end_date) as rpm_lease_end_month,
day(V.lease_end_date) as rpm_lease_end_day,
unix_timestamp(V.lease_start_date, 'yyy-mm-dd') as rpm_lease_start_ts,
unix_timestamp(V.lease_end_date, 'yyy-mm-dd') as rpm_lease_end_ts,
pv.corporation as polk_corporation,
pv.report_year_month as polk_report_year_month,
pv.transaction_date as polk_transaction_date,
unix_timestamp(pv.transaction_date, 'yyyyMMdd') as polk_transaction_ts,
pv.trans_price as polk_trans_price,
pv.data_type as polk_data_type,
pv.origin as polk_origin,
pv.purchase_lease as polk_purchase_lease,
pv.vehicle_count as polk_vehicle_count,
pv.dealer_name as polk_dealer_name,
pv.dealer_address as polk_dealer_address,
pv.dealer_town as polk_dealer_town,
pv.dealer_state as polk_dealer_state,
pv.dealer_zip as polk_dealer_zip,
pv.dealer_dma as polk_dealer_dma,
pv.fran_ind as polk_fran_ind
cast(ovt_reg.pur_amt as int) as pur_amt,
cast(ovt_reg.at_sale_nat_mmr as int) as at_sale_nat_mmr,
ovt_reg.gross_txn_flg as gross_txn_flg,
ovt_reg.offrng_flg as offrng_flg,
ovt_reg.offered_cnt as offered_cnt,
ovt_reg.check_in_ts as check_in_ts,
ovt_reg.preview_ts as preview_ts,
ovt_reg.act_offrng_start1_ts as act_offrng_start1_ts,
ovt_reg.act_offrng_end1_ts as act_offrng_end1_ts,
ovt_reg.auction_key as auction_key,
ovt_reg.pur_type_key as pur_type_key,
ovt_reg.at_reg_nat_mmr as at_reg_nat_mmr,
ovt_reg.at_reg_rgn_mmr as at_reg_rgn_mmr,
ovt_reg.gross_pur_amt as gross_pur_amt,
ovt_reg.buyer_fees_amt as buyer_fees_amt,
ovt_reg.net_seller_amt as net_seller_amt,
ovt_reg.arbitrated_amt as arbitrated_amt,
ovt_reg.reg_cr_grade as reg_cr_grade,
ovt_reg.reg_cr_grade1_ts as reg_cr_grade1_ts,
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
ovt_reg.reg_cr_grade as reg_cr_grade,
from  rpm.purchases_stg P 
join rpm.vehicles_stg V on P.vehicle_id = V.id
left join vdm.vehicles vdmv on vdmv.vb_vin=v.vin 
left join rpm.aim_vehicles_stg AV on V.id=AV.vehicle_id
left join mmr.sales mmr on V.vin = mmr.m_vin
left join (select aim_vehicle_id, SUM(estimated_repair_cost) as repair_cost from  rpm.aim_damages_stg GROUP BY aim_vehicle_id) AD on AD.aim_vehicle_id=AV.id
join (select *,  datediff( from_unixtime(unix_timestamp()), to_date(created_at)) as stockage from rpm.groundings_stg) G on G.vehicle_id = V.id
join rpm.dealerships_stg D on D.nna_dealer_number=V.dealer_number
left join vdm.vdm_options_packages vdmo on v.vin = vdmo.vin
left join 3rd_party.polk_filtered pv on pv.vin = v.vin
left join ovt.man_ovt_fact_registration ovt on ovt.vin = v.vin;
SET spark.sql.shuffle.partitions=1;