CREATE EXTERNAL TABLE `txf_sales`(
  `vin` string, 
  `customer_id` bigint, 
  `model` string, 
  `invoice_date` date, 
  `price` decimal(38,5), 
  `created_at` timestamp, 
  `load_timestamp` timestamp)
PARTITIONED BY ( 
  `tran_date` date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://s3-datalake-dev/silver-txf/txf_sales/'
TBLPROPERTIES (
  'CreatedByJob'='ingest_txf_sales', 
  'CreatedByJobRun'='jr_e19c06b68243359497552a94c40df9212607d270dde41d0708a577d6c793b3a6', 
  'UpdatedByJob'='ingest_txf_sales', 
  'UpdatedByJobRun'='jr_7beae22b533f24cb528972e749917140d3475936348949471da64f5ad658819d', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')