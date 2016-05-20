SET spark.sql.shuffle.partitions=200;
set mapred.tasktracker.expiry.interval=1800000;
set mapreduce.task.timeout=1800000;
set mapred.task.timeout= 1800000;
set mapred.max.split.size=80000000;
set mapreduce.input.fileinputformat.split.maxsize=80000000;
set mapred.reduce.slowstart.completed.maps=1;
set mapred.reduce.tasks=32;
use vauto;
 SET spark.sql.thriftserver.scheduler.pool=background;
set hive.support.quoted.identifiers=none;
set mapred.reduce.slowstart.completed.maps=1;
set mapred.reduce.tasks=32;

drop table if exists  vauto.vauto_recent_market_data_dedup_inc;
CREATE TABLE vauto.vauto_recent_market_data_dedup_inc  AS SELECT 
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
from (
select `(group_rank|last_seen_ts)?+.+` from vauto.vauto_recent_market_data_dedup
union 
select `(themonth|theyear|theday)?+.+`  from vauto.vauto_recent_market_data_new_parts where cast(model_year as int) >=2010 and vin is not null and created is not null and is_certified is not null
)  t ) r where r.group_rank=1;


drop table if exists  vauto.vauto_sold_market_vehicle_dedup_inc;
CREATE TABLE vauto.vauto_sold_market_vehicle_dedup_inc  AS SELECT 
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
from (
select `(group_rank|last_seen_ts)?+.+` from vauto.vauto_sold_market_vehicle_dedup
union 
select `(themonth|theyear|theday)?+.+` from vauto.vauto_sold_market_vehicle_new_parts where cast(model_year as int) >=2010 and vin is not null and created is not null and is_certified is not null
)  t ) r where r.group_rank=1;


drop table vauto.vauto_sold_market_vehicle_dedup;
alter table vauto.vauto_sold_market_vehicle_dedup_inc rename to  vauto.vauto_sold_market_vehicle_dedup;
drop table vauto.vauto_recent_market_data_dedup;
alter table vauto.vauto_recent_market_data_dedup_inc rename to vauto.vauto_recent_market_data_dedup;
