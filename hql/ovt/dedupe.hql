use ovt;
set hive.exec.counters.pull.interval =500;
SET spark.sql.shuffle.partitions=200;
set mapred.tasktracker.expiry.interval=1800000;
set mapreduce.task.timeout=1800000;
set mapred.reduce.tasks=600;
set mapred.task.timeout= 1800000;
set mapred.max.split.size=80000000;
set mapreduce.input.fileinputformat.split.maxsize=80000000;
set mapred.reduce.slowstart.completed.maps=1;
set mapred.reduce.tasks=256;

drop table if exists ovt.man_ovt_fact_registration_dedup;
create table if not exists ovt.man_ovt_fact_registration_dedup as select
*
from ( select
*,
row_number() over ( partition by uniq_reg_id order by ACT_OFFRNG_START_TS desc , DW_UPDATED_ON  desc ) as group_rank
from ovt.man_ovt_fact_registration where model_yr>=2010 and uniq_reg_id is not null and uniq_reg_id != 'null'
) t where group_rank=1;

drop table if exists ovt.man_ovt_fact_registration_ext_dedup;
create table if not exists ovt.man_ovt_fact_registration_ext_dedup STORED as PARQUET as
select * from (
select *,
row_number() over ( partition by reg_key  order by  DW_UPDATED_ON  desc ) as group_rank
 from ovt.man_ovt_fact_registration_ext where reg_key is not null
) t where t.group_rank=1;

drop table if exists ovt.man_ovt_dim_auction_dedup;
create table if not exists ovt.man_ovt_dim_auction_dedup as select *
from
(select  *, row_number() over (partition by auction_key order by  DW_UPDATED_ON  desc ) group_rank from ovt.man_ovt_dim_auction where auction_key is not null  ) t  where group_rank=1;

drop table if exists ovt.man_ovt_dim_flndr_dedup;
create table if not exists ovt.man_ovt_dim_flndr_dedup as select *
from
(select  *, row_number() over (partition by flndr_key) as group_rank from ovt.man_ovt_dim_flndr where flndr_key is not null  ) t  where group_rank=1;

drop table if exists ovt.man_ovt_dim_make_model_trim_dedup;
create table if not exists ovt.man_ovt_dim_make_model_trim_dedup as select *
from
(select  *, row_number() over (partition by make_model_trim_key order by  DW_UPDATED_ON  desc ) as group_rank from ovt.man_ovt_dim_make_model_trim  where make_model_trim_key  is not null  ) t  where group_rank=1;

drop table if exists ovt.man_ovt_dim_customer_dedup;
create table if not exists ovt.man_ovt_dim_customer_dedup as select *
from
(select  *, row_number() over (partition by cust_key order by  DW_UPDATED_ON  desc ) as group_rank from ovt.man_ovt_dim_customer  where cust_key  is not null  ) t  where group_rank=1;
