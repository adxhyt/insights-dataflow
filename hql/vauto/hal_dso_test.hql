drop table if exists vauto.hal_historical;
create table vauto.hal_historical 
stored as orc tblproperties ("orc.compress"="SNAPPY")  as select 
coalesce(s.vin, rm.vin) as vin,
coalesce(s.postal_code, rm.postal_code) as postal_code,
coalesce(s.stock_number, rm.stock_number) as stock_number,
coalesce(s.model_year, rm.model_year) as model_year,
coalesce(s.make, rm.make) as make,
coalesce(s.model, rm.model) as model,
coalesce(s.series, rm.series) as series,
coalesce(s.series_detail, rm.series_detail) as series_detail,
coalesce(s.odometer, rm.odometer) as odometer,
coalesce(s.new_used, rm.new_used) as new_used,
coalesce(s.is_certified, rm.is_certified) as is_certified,
coalesce(s.body_description, rm.body_description) as body_description,
coalesce(s.body_type, rm.body_type) as body_type,
coalesce(s.body_door_count, rm.body_door_count) as body_door_count,
coalesce(s.body_cab_style, rm.body_cab_style) as body_cab_style,
coalesce(s.body_bed_style, rm.body_bed_style) as body_bed_style,
coalesce(s.body_roof_style, rm.body_roof_style) as body_roof_style,
coalesce(s.engine_description, rm.engine_description) as engine_description,
coalesce(s.engine_cylinder_count, rm.engine_cylinder_count) as engine_cylinder_count,
coalesce(s.engine_displacement, rm.engine_displacement) as engine_displacement,
coalesce(s.engine_fuel_type, rm.engine_fuel_type) as engine_fuel_type,
coalesce(s.transmission_description, rm.transmission_description) as transmission_description,
coalesce(s.transmission_type, rm.transmission_type) as transmission_type,
coalesce(s.transmission_gear_count, rm.transmission_gear_count) as transmission_gear_count,
coalesce(s.drive_train_type, rm.drive_train_type) as drive_train_type,
coalesce(s.exterior_color, rm.exterior_color) as exterior_color,
coalesce(s.exterior_base_color, rm.exterior_base_color) as exterior_base_color,
coalesce(s.interior_description, rm.interior_description) as interior_description,
coalesce(s.interior_color, rm.interior_color) as interior_color,
coalesce(s.interior_material, rm.interior_material) as interior_material,
coalesce(s.categorized_equipment_ids, rm.categorized_equipment_ids) as categorized_equipment_ids,
coalesce(s.days_ininventory, rm.days_ininventory) as days_ininventory,
coalesce(s.veh_segment, rm.veh_segment) as veh_segment,
coalesce(s.veh_type, rm.veh_type) as veh_type,
coalesce(s.created, rm.created) as created,
 case 
   when unix_timestamp(coalesce(s.created, rm.created), 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(coalesce(rm.created,s.created), 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
   else unix_timestamp(coalesce(rm.created,s.created), 'MM/dd/yyyy HH:mm:ss aa') end as created_ts, 
coalesce(s.last_seen, rm.last_seen) as last_seen,
coalesce(s.last_seen_ts, rm.last_seen_ts) as last_seen_ts,
case when  s.vin is not null then 1 else 0 end as issold
from vauto.vauto_recent_market_data_dedup rm  
full join vauto.vauto_sold_market_vehicle_dedup s  on  rm.vin = s.vin and rm.created = s.created and rm.is_certified=s.is_certified;

