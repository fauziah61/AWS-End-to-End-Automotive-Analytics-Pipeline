CREATE EXTERNAL TABLE `mart_sales`(
  `periode` string, 
  `class` string, 
  `model` string, 
  `total` decimal(38,5), 
  `load_timestamp` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://s3-datalake-dev/gold-mart/mart_sales/'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'CreatedByJob'='ingest_mart_sales', 
  'CreatedByJobRun'='jr_4a457a72d5c870432e860e910268f35f7f0b236e55fc928f757f1a01738ef95c', 
  'classification'='parquet', 
  'last_modified_by'='hadoop', 
  'last_modified_time'='1792244430', 
  'numFiles'='3', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='2984', 
  'useGlueParquetWriter'='true')