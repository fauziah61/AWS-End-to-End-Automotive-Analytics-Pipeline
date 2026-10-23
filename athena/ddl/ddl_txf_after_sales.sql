CREATE EXTERNAL TABLE `txf_after_sales`(
  `service_type` string COMMENT '', 
  `service_date` date COMMENT '', 
  `service_ticket` string COMMENT '', 
  `created_at` timestamp COMMENT '', 
  `vin` string COMMENT '', 
  `model` string COMMENT '', 
  `customer_id` bigint COMMENT '', 
  `load_timestamp` timestamp COMMENT '')
PARTITIONED BY ( 
  `tran_date` date COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://s3-datalake-dev/silver-txf/txf_after_sales/'
TBLPROPERTIES (
  'CreatedByJob'='ingest_txf_after_sales', 
  'CreatedByJobRun'='jr_1b52bd0fb604e41376aa42b9677f42ce21c117687dc015f73194f26192438a00', 
  'UpdatedByJob'='ingest_txf_after_sales', 
  'UpdatedByJobRun'='jr_924ba4623334b34f1c698798a7c01c4e82b4f55bea3da821927d4272dd80fbd1', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')