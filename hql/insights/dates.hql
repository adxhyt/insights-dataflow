use insights;
drop table if exists dates;
create external table dates (date string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/insights/dates/';
