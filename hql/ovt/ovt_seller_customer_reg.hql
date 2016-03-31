use ovt ;

SET spark.sql.shuffle.partitions=128;
drop table if exists  ovt_seller_customer_reg_tmp;

create table ovt_seller_customer_reg_tmp as select 
ovt_reg.reg_key as reg_key,
seller_cust.cur_cust_nm as vendortradingname,
seller_cust.country_cd as vendorcountryid,
seller_cust.country_cd as vendorcountryname,
seller_cust.cust_id as vendorid,
buyer_cust.cur_cust_nm as buyer,
buyer_cust.cust_id as buyerid, 
buyer_cust.cur_cust_nm as buyercode,
buyer_cust.country_cd as buyercountry,
buyer_cust.country_cd as buyercountryid
from 
 ovt.man_ovt_fact_registration_dedup ovt_reg
left join  ovt.man_ovt_dim_customer_dedup buyer_cust on ovt_reg.buyer_cust_key = buyer_cust.cust_key and buyer_cust.cust_key >= 0
left join ovt.man_ovt_dim_customer_dedup seller_cust on ovt_reg.seller_cust_key = seller_cust.cust_key and seller_cust.cust_key >=0;

create table if not exists ovt.ovt_seller_customer_reg_dedup
as
select *
from
(select *, row_number() over (partition by reg_key) as group_rank
from ovt.ovt_seller_customer_reg_tmp
) t where group_rank=1;

drop table ovt_seller_customer_reg_tmp;
