--
-- Hive script to import Experian origination data, received as a one-time snapshot from Experian
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
-- Below is a sample excerpt from the Experian data file
-- "TitleState"|"ReportingPeriod"|"GeoLevel"|"VehicleYear"|"VehicleMake"|"VehicleModel"|"VehicleSegment"|"LeaseMaturityDate"|"TotalEstimatedMarketCount"
-- "AK"|"201201 - 201510"|"ANCHORAGE, AK"|"2011"|"BMW"|"3-SERIES"|"UPSCALE - NEAR LUXURY"|"Nov 2014"|"7"

SET spark.default.parallelism=8;
-- Make table for new updated data
add jar hdfs://rmsus-hdn01:8020/user/oozie/share/lib/hive-serde.jar ;
drop  table if exists `experian.originations_new`;
CREATE EXTERNAL TABLE `experian.originations_new`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_maturity_date` string ,
`estimated_market_cnt` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "|",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
LOCATION '/data/database/experian/originations/new'
TBLPROPERTIES("skip.header.line.count"="1");

-- Make table for the old and the new
drop  table if exists `experian.originations_stg`;
CREATE TABLE `experian.originations_stg`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_maturity_date` string ,
`estimated_market_cnt` int
);

-- Update the old data with new estimated_market_cnt
insert into experian.originations_stg
select o_old.state_abbr,
o_old.reporting_period,
o_old.experian_dma,
o_old.modelyear,
o_old.make,
o_old.model,
o_old.segment,
o_old.lease_maturity_date,
o_new.estimated_market_cnt from experian.originations_old o_old
left join experian.originations_new o_new on
o_old.state_abbr=o_new.state_abbr and
o_old.experian_dma=o_new.experian_dma and
o_old.modelyear=o_new.modelyear and
o_old.make=o_new.make and
o_old.model=o_new.model and
o_old.segment=o_new.segment and
o_old.lease_maturity_date=o_new.lease_maturity_date;

-- Insert new rows from the new data
insert into experian.originations_stg
select o_new.state_abbr,
o_new.reporting_period,
o_new.experian_dma,
o_new.modelyear,
o_new.make,
o_new.model,
o_new.segment,
o_new.lease_maturity_date,
o_new.estimated_market_cnt 
from experian.originations_new o_new 
left join experian.originations_old o_old on
o_old.state_abbr=o_new.state_abbr and
o_old.experian_dma=o_new.experian_dma and
o_old.modelyear=o_new.modelyear and
o_old.make=o_new.make and
o_old.model=o_new.model and
o_old.segment=o_new.segment and
o_old.lease_maturity_date=o_new.lease_maturity_date
where o_old.state_abbr is null;

cache table `experian.originations_stg`;

-- Store the new table for the next months update
drop  table if exists `experian.originations_old`;
CREATE TABLE `experian.originations_old`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_maturity_date` string,
`estimated_market_cnt` int
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n' STORED AS TEXTFILE LOCATION '/data/database/experian/originations/old';
INSERT INTO TABLE experian.originations_old
SELECT * from experian.originations_stg;

--
-- OpenCSV serde produces text columns irrespective of the table specification.
-- Here we convert columns to their ultimate types and also add new columns as necessary
--

drop  table if exists `originations`;
CREATE TABLE `originations`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_maturity_date` string ,
`estimated_market_cnt` int,
`reporting_period_ts` int,
`reporting_period_year` int,
`reporting_period_month` int,
`lease_maturity_date_ts` int,
`lease_maturity_date_year` int,
`lease_maturity_date_month` int,
`dma_id` int        COMMENT 'AutoTrader DMA ID that corresponds to shapes',
`dma_desc` string   COMMENT'DMA Description from At Geo - human readable'
);

INSERT INTO TABLE originations 
SELECT
  state_abbr,
  reporting_period,
  experian_dma,
  modelyear,
  case when make='UNKNOWN VIN' then NULL else make end as make,
  case when model='UNKNOWN VIN' then 'UNKNOWN MODEL' else model end as model,
  segment,
  lease_maturity_date,
  estimated_market_cnt,
  unix_timestamp(SUBSTRING(reporting_period, 10, 6), "yyyyMM"),
  year(cast(unix_timestamp(SUBSTRING(reporting_period, 10, 6), "yyyyMM") * 1000 as timestamp)),
  month(cast(unix_timestamp(SUBSTRING(reporting_period, 10, 6), "yyyyMM") * 1000 as timestamp)),
  unix_timestamp(lease_maturity_date, "MMM yyyy"),
  year(cast(unix_timestamp(lease_maturity_date, "MMM yyyy") * 1000 as timestamp)),
  month(cast(unix_timestamp(lease_maturity_date, "MMM yyyy") * 1000 as timestamp)),
  GEO.dma_at_geo_id as dma_id,
  GEO.dma_at_geo_desc as dma_desc
FROM originations_stg
left outer join experian.dma_mapping as GEO on experian_dma=GEO.dma_experian
;

cache table originations;

drop table originations_stg;
