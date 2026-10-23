CREATE EXTERNAL TABLE `customer_address_csv`(
  `id` string COMMENT 'from deserializer', 
  `customer_id` string COMMENT 'from deserializer', 
  `address` string COMMENT 'from deserializer', 
  `city` string COMMENT 'from deserializer', 
  `province` string COMMENT 'from deserializer', 
  `created_at` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'com.amazonaws.glue.serde.GrokSerDe' 
WITH SERDEPROPERTIES ( 
  'input.format'='^%{DATA:id}\\|@~%{DATA:customer_id}\\|@~%{DATA:address}\\|@~%{DATA:city}\\|@~%{DATA:province}\\|@~%{GREEDYDATA:created_at}$') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://s3-datalake-dev/bronze-stg/customer_address_csv/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='87c2ec4a-12e1-4d4e-8687-8cc9ca25137d', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler_s3_raw', 
  'averageRecordSize'='88', 
  'classification'='customer-address-multidelimiter', 
  'compressionType'='none', 
  'grokPattern'='^%{DATA:id}\\|@~%{DATA:customer_id}\\|@~%{DATA:address}\\|@~%{DATA:city}\\|@~%{DATA:province}\\|@~%{GREEDYDATA:created_at}$', 
  'objectCount'='2', 
  'recordCount'='2051', 
  'sizeKey'='180554', 
  'typeOfData'='file')