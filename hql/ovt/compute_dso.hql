use ovt;
drop table ovt_make_model;
SET spark.sql.shuffle.partitions=128;
create table if not exists  ovt_make_model as select 
mmt.make_desc as make,
 mmt.model_desc as model,                       
 r.model_yr as model_year,                      
 r.offered_cnt,
 r.reg_ts,
 r.auction_key, 
unix_timestamp(r.reg_ts) as reg_ts_ts,
 concat(year(r.reg_ts), month(r.reg_ts)) as reg_year_month,
 r.vin,
 r.reg_key,
 r.gross_txn_flg,
r.offrng_flg,
r.pur_amt,
r.at_sale_nat_mmr
 from ovt.man_ovt_fact_registration_dedup r  join ( 
select make_model_trim_key, make_desc , model_desc from ovt.man_ovt_dim_make_model_trim group by make_model_trim_key, make_desc, model_desc) mmt
 on mmt.make_model_trim_key = r.make_model_trim_key;

drop table make_model_metrics_tmp;
create table  if not exists make_model_metrics_tmp as select 
   make, 
   model,
   model_year, 
   auction_key,
   reg_year_month,
  count(distinct case when gross_txn_flg=1 then  vin else null end) as gross_txn,
  sum(offered_cnt) as sum_offered_cnt,
  count(offered_cnt) as count_offered_cnt,
  sum( case offrng_flg when 1 then 1 else 0 end  ) as sum_offerng_flg, 
  sum( case pur_amt when 'null' then 0 else pur_amt end ) as mmr_retention_sum_pur_amt,
  sum(  case at_sale_nat_mmr when 'null' then 0 else at_sale_nat_mmr end ) as mmr_retention_sum_nat_mmr
  from  ovt_make_model group  by make, model, model_year,auction_key, reg_year_month;
   
drop table if exists make_model_metrics;
create  table make_model_metrics as select 
 ovt_make_model.reg_key,
 ovt_make_model.make,
 ovt_make_model.model,
 ovt_make_model.model_year,
 ovt_make_model.reg_ts_ts,
 metrics.gross_txn/metrics.sum_offerng_flg  as effectiveness,
 metrics.gross_txn/sum_offered_cnt as efficiency,
 metrics.gross_txn as sum_gross_txn,
 metrics.sum_offered_cnt  as sum_offered_cnt,
 metrics.sum_offerng_flg  as sum_offerng_flg,
 metrics.mmr_retention_sum_pur_amt as sum_pur_amt,
 metrics.mmr_retention_sum_nat_mmr  as sum_sale_nat_mmr,
 metrics.mmr_retention_sum_pur_amt/metrics.mmr_retention_sum_nat_mmr  as mmr_retention 
from ovt_make_model join make_model_metrics_tmp metrics on  
metrics.model = ovt_make_model.model and  
metrics.make = ovt_make_model.make and 
metrics.model_year = ovt_make_model.model_year and 
metrics.auction_key = ovt_make_model.auction_key and 
metrics.reg_year_month = ovt_make_model.reg_year_month;
 
drop table if exists ovt.make_model_metrics_dedup;
create table if not exists ovt.make_model_metrics_dedup  as select *
from
(select  *, row_number() over (partition by reg_key ) as group_rank from ovt.make_model_metrics ) t  where group_rank=1;
