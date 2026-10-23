CREATE EXTERNAL TABLE `txf_customers_address`(
  `id` bigint COMMENT '', 
  `customer_id` bigint COMMENT '', 
  `address` string COMMENT '', 
  `city` string COMMENT '', 
  `province` string COMMENT '', 
  `created_at` timestamp COMMENT '', 
  `load_timestamp` timestamp COMMENT '')
PARTITIONED BY ( 
  `date_process` date COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://s3-datalake-dev/silver-txf/txf_customers_address/'
TBLPROPERTIES (
  'CreatedByJob'='nb_ingest_stg_customer_address', 
  'CreatedByJobRun'='nb_ingest_stg_customer_address', 
  'UpdatedByJob'='ingest_stg_txf_customer_address', 
  'UpdatedByJobRun'='jr_f28399adacec4925a0a08a82c64a8c33b9d7235c8335a66ff6b1064afbc09a82', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')