drop table if exists ais.postal_codes;
CREATE EXTERNAL TABLE `ais.postal_codes`(
`codes` int,
`RegionID` int
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n' 
STORED AS TEXTFILE 
LOCATION '/data/database/ais/postal_codes/' 
tblproperties("skip.header.line.count"="1");