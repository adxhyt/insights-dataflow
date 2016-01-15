SET spark.sql.autoBroadcastJoinThreshold= 104857600;
SET spark.sql.shuffle.partitions=200;


INSERT INTO TABLE chrome.chrome_consolidated
SELECT
 vd.input_row_num,
 s.passthru_id,
 s.style_id,
 s.base_msrp,
 s.base_invoice,
 s.dest_chrg,
 s.is_price_unknown,
 s.market_class_id,
 s.model_year,
 s.style_name,
 s.style_name_wo_trim,
 s.trim_name,
 s.mfr_model_code,
 s.is_fleet_only,
 s.is_model_fleet,
 s.pass_doors,
 s.alt_model_name,
 s.alt_style_name,
 s.alt_body_type,
 s.drivetrain,
 md.model_id,
 md.model_name,
 md.country,
 md.language,
 sdd.sub_division_id,
 sdd.sub_division_name,
 dd.division_id,
 dd.division_name,
 vd.vin, 
 vd.gvwr_low, 
 vd.gvwr_high, 
 vd.world_manufacturer_identifier, 
 vd.manufacturer_identification_code, 
 vd.restraint_types, 
 vd.division, 
 vd.body_type, 
 vd.driving_wheels, 
 vd.built, 
 vd.best_make_name, 
 vd.best_model_name, 
 vd.best_style_name, 
 vd.best_trim_name, 
 vd.base_msrp_low, 
 vd.base_msrp_high, 
 vd.base_inv_low, 
 vd.base_inv_high, 
 vd.dest_chrg_low, 
 vd.dest_chrg_high, 
 e.engine_type_id, 
 e.engine_type, 
 e.fuel_type_id, 
 e.fuel_type, 
 e.hp_value, 
 e.hp_rpm, 
 e.net_torque_value, 
 e.net_torque_rpm, 
 e.cylinders, 
 e.displ_liters, 
 e.displ_cubic_inch, 
 e.fuel_economy_city_low, 
 e.fuel_economy_city_high, 
 e.fuel_economy_hwy_low, 
 e.fuel_economy_hwy_high, 
 e.fuel_economy_unit, 
 e.fuel_tank_capacity_low, 
 e.fuel_tank_capacity_high, 
 e.fuel_tank_capacity_unit, 
 e.forced_induction_id, 
 e.forced_induction_type, 
 e.install_cause, 
 e.install_cause_detail, 
 e.is_high_output
from chrome.vehicle_description vd 
join chrome.style s on  vd.input_row_num = s.input_row_num
 JOIN chrome.engine as e ON e.input_row_num = vd.input_row_num
 JOIN chrome.subdivision_definition as sdd ON ( s.sub_division_id = sdd.sub_division_id)
 JOIN chrome.division_definition as dd ON (s.division_id = dd.division_id)
 JOIN chrome.model_definition as md ON ( s.model_id = md.model_id)
 JOIN chrome.body_type as bt ON (bt.style_id = s.style_id);
