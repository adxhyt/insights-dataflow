SET spark.sql.shuffle.partitions=200;
use vauto;
 SET spark.sql.thriftserver.scheduler.pool=background;

drop table if exists  vauto_recent_market_data_dedup;
CREATE TABLE vauto_recent_market_data_dedup  AS SELECT 
*
from  
( SELECT 
*,
 case 
   when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
   else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end as last_seen_ts, 
row_number() over ( partition by vin, created, is_certified order by
        case when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
        else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end desc ) group_rank 
from vauto_recent_market_data where cast(model_year as int) >=2012 and vin is not null and created is not null and is_certified is not null ) rm  where rm.group_rank=1;

drop table if exists  vauto.vauto_sold_market_vehicle_dedup;
CREATE TABLE vauto.vauto_sold_market_vehicle_dedup  AS SELECT 
*
from  
( SELECT 
*,
 case 
   when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
   else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end as last_seen_ts, 
row_number() over ( partition by vin, created, is_certified  order by
        case when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
        else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end desc ) group_rank 
from vauto.vauto_sold_market_vehicle where cast(model_year as int) >=2012 and vin is not null and created is not null and is_certified is not null ) rm  where rm.group_rank=1;