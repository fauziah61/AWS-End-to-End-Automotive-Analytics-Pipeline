CREATE EXTERNAL TABLE `mart_cust_service_prio`(
  `priority` string, 
  `periode` int, 
  `vin` string, 
  `count_service` int, 
  `customer_name` string, 
  `address` string, 
  `load_timestamp` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://s3-datalake-dev/gold-mart/mart_cust_service_prio/'
TBLPROPERTIES (
  'CreatedByJob'='ingest_mart_cust_service_prio', 
  'CreatedByJobRun'='jr_3d804f2c0793b8bf6a65d2bcefeb19ea940e069cb92d6dbe361951962b96be62', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')