use insights;
drop table IF EXISTS retail_market_cached;
CREATE  TABLE IF NOT EXISTS `retail_market_cached`
(      
vin                             STRING,
postal_code                     INT,
stock_number                    STRING,
modelyear                      INT,
make                            STRING,
model                           STRING,
series                          STRING,
series_detail                   STRING,
odometer                        INT,
new_used                        STRING,
is_certified                    INT,
body_description                STRING,
body_type                       STRING,
number_of_doors                 INT,
body_cab_style                  STRING,
body_bed_style                  STRING,
body_roof_style                 STRING,
engine_description              STRING,
engine_cylinder_count           INT,
engine_displacement             FLOAT,
engine_fuel_type_desct                STRING,
transmission_description        STRING,
transmission_type               STRING,
transmission_gear_count         INT,
drive_train_type                STRING,
exteriorcolour                  STRING,
exterior_base_color             STRING,
interior_description            STRING,
interior_color                  STRING,
interior_material               STRING,
categorized_equipment_ids       STRING,
stockageweeks                   INT,
veh_segment                     STRING,
veh_type                        STRING,
market_created                         BIGINT,
market_last_seen                       BIGINT,
sales_last_seen                 BIGINT,
issold                          INT,
geo_dma_durable_key             STRING,
geo_dma_code                    STRING,
geo_dma_desc                    STRING,
geo_city                        STRING,
geo_state_code                  STRING,
geo_county                      STRING,
geo_country_code                STRING,
geo_latitude                    DOUBLE,
geo_longitude                   DOUBLE,
geo_submarket                   STRING,
geo_tim_zone_desc               STRING,
geo_dma_id                      SMALLINT
)
STORED AS PARQUET;
