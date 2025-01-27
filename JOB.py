  CREATE EXTERNAL TABLE `wewy`(
  `prac_id` string,
  `karta_okz` string,
  `status_id` string,
  `nr_czytnika` string
  )
  PARTITIONED BY (data_czas string)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
  's3://668704778226-formatted-data/'
  TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0',
  'CrawlerSchemaSerializerVersion'='1.0',
  'UPDATED_BY_CRAWLER'='review_crawler',
  'averageRecordSize'='1459',
  'classification'='parquet',
  'compressionType'='none',
  'objectCount'='1',
  'recordCount'='100000',
  'sizeKey'='94807077',
  'typeOfData'='file')