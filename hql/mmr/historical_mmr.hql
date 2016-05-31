use mmr;
DROP TABLE IF EXISTS man_historic_mmr;
CREATE EXTERNAL TABLE IF NOT EXISTS man_historic_mmr
(
MID    STRING,
N_ALL    INT,
N_360d    INT,
N_30d    INT,
age_1    FLOAT,
age_2    INT,
mile_factor    FLOAT,
YEAR    INT,
MAKE    STRING,
MODEL    STRING,
BODY    STRING,
GoodPrice    INT,
AuctionAvgMile    INT,
StdDevPrice    INT,
StdDevMileage    INT,
EditionDate    STRING
)
PARTITIONED BY (
  `edition_year` smallint
 )
STORED AS PARQUET 
LOCATION  '/data/database/mmr/man_historic_mmr/'
;
