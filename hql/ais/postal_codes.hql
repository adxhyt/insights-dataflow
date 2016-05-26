use ais;
drop table if exists postal_codes;
CREATE EXTERNAL TABLE `postal_codes`(
`codes` int,
`RegionID` int
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n' 
STORED AS TEXTFILE 
LOCATION '/data/database/ais/postalCode/' 
tblproperties("skip.header.line.count"="1");