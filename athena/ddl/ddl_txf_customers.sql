CREATE EXTERNAL TABLE `txf_customers`(
  `id` bigint COMMENT '', 
  `name` string COMMENT '', 
  `dob` date COMMENT '', 
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
  's3://s3-datalake-dev/silver-txf/txf_customers/'
TBLPROPERTIES (
  'CreatedByJob'='ingest_txf_customers', 
  'CreatedByJobRun'='jr_4841d9726df45c36bb61a13918d52125643dc189faef372844c453053d776659', 
  'UpdatedByJob'='ingest_txf_customers', 
  'UpdatedByJobRun'='jr_3fa2eff347004398434a938a8db0b80714223b75161ff52b74ee1daf44525bc8', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')