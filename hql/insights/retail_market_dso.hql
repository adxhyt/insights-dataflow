use insights;
set mapred.reduce.tasks = 10;
set  hive.auto.convert.join=false;
set mapred.tasktracker.expiry.interval=1800000;
set mapreduce.task.timeout=1800000;
set mapred.task.timeout= 1800000;
set mapred.max.split.size=80000000;
set mapreduce.input.fileinputformat.split.maxsize=80000000;
set mapreduce.job.reduce.slowstart.completedmaps=1;
set  spark.sql.shuffle.partitions=10;
set spark.network.timeout=99999s;

set spark.shuffle.blockTransferService=nio;
drop table if exists dso_dates_tmp;
create table if not exists  dso_dates_tmp as select * from(
select distinct to_date(from_unixtime(market_created)) as dt from retail_market_cached union
select distinct to_date(from_unixtime(market_last_seen)) as dt from retail_market_cached union
select distinct to_date(from_unixtime(sales_last_seen)) as dt from retail_market_cached) t;
set  spark.sql.shuffle.partitions=600;
set mapred.reduce.tasks = 600;

drop  table if exists dso_sold_daily_tmp;
create table if not exists dso_sold_daily_tmp as select 
   make,
   model,
   geo_dma_id,
   geo_dma_desc,
   modelyear,  
   veh_segment,
   is_certified,
   body_type,
   body_description,
   dd.dt,
   count(distinct case when issold =1 then vin else null end)  as count_sold
   from  dso_dates_tmp DD left join  retail_market_cached RM
   on to_date(from_unixtime(RM.sales_last_seen)) = DD.dt 
 group by body_type, body_description, is_certified, veh_segment, make, model, geo_dma_desc, geo_dma_id, modelyear,   DD.dt  order by DD.dt  desc;

drop table dso;
create table if not exists dso as 
select 
make,
model, 
modelyear, 
geo_dma_id, 
geo_dma_desc,
veh_segment,
is_certified,
body_type,
body_description,
DD.dt, 
count(distinct RM.vin) as count from
dso_dates_tmp DD join  retail_market_cached RM
on
  RM.market_created <= unix_timestamp(DD.dt, 'yyyy-MM-dd') and unix_timestamp(DD.dt, 'yyyy-MM-dd') <= RM.market_last_seen 
group by  
veh_segment, 
is_certified,
make, 
model,
modelyear, 
geo_dma_id,
geo_dma_desc,
body_type,
body_description,
 DD.dt;

drop table dso_metrics_pre;
create table if not exists dso_metrics_pre as select 
dso.make, 
dso.model,
dso.modelyear,
dso.geo_dma_id,
dso.geo_dma_desc,
dso.dt as date,
dso.veh_segment as segment, 
dso.is_certified,
dso.body_type,
dso.body_description,
coalesce(dso.count, 0) as inventory,
coalesce(ds.count_sold, 0) as daily_sold
from  
dso full join dso_sold_daily_tmp ds  on
dso.make = ds.make and
dso.model = ds.model and
dso.modelyear = ds.modelyear and 
dso.geo_dma_id = ds.geo_dma_id and 
dso.dt = ds.dt and
dso.is_certified = ds.is_certified and
dso.veh_segment=ds.veh_segment and 
dso.body_type=ds.body_type and
dso.geo_dma_desc = ds.geo_dma_desc and
dso.body_description=ds.body_description 
where dso.count != 0 order by dso.dt;

drop  table if exists dso_metrics_tmp;
create table if not exists dso_metrics_tmp as select 
   make,
   model,
   modelyear,  
   geo_dma_id,
   geo_dma_desc,
   segment,
   is_certified,
   body_type,
   body_description,
   date,
   unix_timestamp(date, 'yyyy-MM-dd') as date_ts,
   inventory,
   sum(coalesce(daily_sold, 0 ) ) over (partition by make, model, modelyear, geo_dma_id,geo_dma_desc,segment, is_certified, body_type, body_description  order by unix_timestamp(date, 'yyyy-MM-dd') range between 3888000 preceding and current row) as count
 from dso_metrics_pre ;

drop  table dso;
drop table dso_dates_tmp;
drop table dso_metrics_pre ;
drop table if exists dso_metrics_bkp;
 
