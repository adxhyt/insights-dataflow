use ais;
drop table if exists programs;
CREATE EXTERNAL TABLE `programs`(
`ProgramID` int,
`ShortTitle` String,
`Title` String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/ais/programs/'
tblproperties ("skip.header.line.count"="1");