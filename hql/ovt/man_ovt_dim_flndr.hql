use ovt;
DROP TABLE IF EXISTS man_ovt_dim_flndr;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_dim_flndr
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/data/database/manheim/man_ovt_raw/man_ovt_snap/dim_flndr'
 TBLPROPERTIES (
    'avro.schema.url'='hdfs://${hiveconf:namenode}:8020/user/oozie/share/hql/avro/create_ovtappadm_dim_flndr_table.avsc');
