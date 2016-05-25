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
set hive.support.quoted.identifiers=none;

drop table if exists ovt.man_ovt_fact_registration_ext_dedup_inc;
create table if not exists ovt.man_ovt_fact_registration_ext_dedup_inc as select
*
from ( select
*,
row_number() over ( partition by reg_key  order by  DW_UPDATED_ON  desc ) as group_rank
from
(
select  `(group_rank)?+.+` from 
 ovt.man_ovt_fact_registration_ext_dedup
union 
select * from man_ovt_fact_registration_ext_new_parts 
) t1
) t where group_rank=1;

drop table ovt.man_ovt_fact_registration_ext_dedup;
alter table ovt.man_ovt_fact_registration_ext_dedup_inc rename to  ovt.man_ovt_fact_registration_ext_dedup;


