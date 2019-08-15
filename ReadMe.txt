
hive> desc carrier_code_orc;
code                    string
description             string
Time taken: 0.07 seconds, Fetched: 2 row(s)



ive> show create table carrier_code_orc;
OK
CREATE TABLE `carrier_code_orc`(
  `code` string,
  `description` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://Hello:9000/user/hive/warehouse/carrier_code_orc'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true',
  'numFiles'='1',
  'numRows'='1492',
  'rawDataSize'='287956',
  'totalSize'='17666',
  'transient_lastDdlTime'='1564789920')
Time taken: 0.118 seconds, Fetched: 18 row(s)
hive>