use ais;
drop table if exists scenarios;
CREATE EXTERNAL TABLE `scenarios`(
`APR` String,
`AdvPgmVehID` int,
`Cash` string,
`Lease` String,
`MSRP` String,
`Make` String,
`Region` int,
`StartDate` String,
`StopDate` String,
`vehicle_year` int,
`vehicle_model` String,
`vehicle_body` String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/ais/scenarios/'
tblproperties ("skip.header.line.count"="1");