use insights;
INSERT OVERWRITE TABLE insights.retail_market_cached_tmp SELECT 
rm.vin                             ,
rm.postal_code                     ,
rm.stock_number                    ,
coalesce(cc.model_year, rm.model_year),
coalesce(cc.best_make_name, rm.make),
coalesce(cc.model_name, cc.best_model_name, rm.model),
rm.series                          ,
rm.series_detail                   ,
rm.odometer                        ,
rm.new_used                        ,
rm.is_certified                    ,
coalesce(cc.body_type_name, rm.body_description),
coalesce(cc.body_type, rm.body_type),
rm.body_door_count                 ,
rm.body_cab_style                  ,
rm.body_bed_style                  ,
rm.body_roof_style                 ,
rm.engine_description              ,
rm.engine_cylinder_count           ,
rm.engine_displacement             ,
rm.engine_fuel_type                ,
rm.transmission_description        ,
rm.transmission_type               ,
rm.transmission_gear_count         ,
coalesce(cc.drivetrain, rm.drive_train_type),
rm.exterior_color                  ,
rm.exterior_base_color             ,
rm.interior_description            ,
rm.interior_color                  ,
rm.interior_material               ,
rm.categorized_equipment_ids       ,
coalesce(s.days_ininventory, rm.days_ininventory)                ,
rm.veh_segment                     ,
coalesce(cc.body_type, rm.veh_type),
 case 
   when unix_timestamp(rm.created, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(rm.created, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
   else unix_timestamp(rm.created, 'MM/dd/yyyy HH:mm:ss aa') end as market_created, 
rm.last_seen_ts as market_last_seen,
s.last_seen_ts as sales_last_seen, 
case when  s.vin is not null then 1 else 0 end as issold,
g.dma_durable_key as geo_dma_durable_key,
g.dma_code as geo_dma_code,
g.dma_desc as geo_dma_desc,
g.dma_id as geo_dma_id,
g.city as geo_city,
g.state_code as geo_state_code,
g.county as geo_county,
g.country_code as geo_country_code,
g.latitude as geo_latitude,
g.longitude as geo_longitude,
g.submarket as geo_submarket,
g.tim_zone_desc as geo_tim_zone_desc,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
'vauto' as source_name,
0 as source_id,
NULL,
NULL
from vauto.vauto_recent_market_data_dedup rm  
left join vauto.vauto_sold_market_vehicle_dedup s  on  rm.vin = s.vin and rm.created = s.created and rm.is_certified=s.is_certified
left join at.geo g on rm.postal_code = cast(g.zip_code as int)
left join chrome.chrome_consolidated cc on rm.vin = cc.vin;

