
use insights;
SET spark.sql.shuffle.partitions=6;

drop table retail_market_cached_bkp;
alter table retail_market_cached rename to retail_market_cached_bkp;
alter table retail_market_cached_tmp rename to retail_market_cached;
uncache table retail_market_cached;
cache table retail_market_cached;

drop table dso_metrics_bkp;
alter table dso_metrics rename to dso_metrics_bkp;
alter table dso_metrics_tmp rename to dso_metrics;
uncache table dso_metrics;
cache table dso_metrics;
