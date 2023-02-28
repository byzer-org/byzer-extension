## mlsql-canal

mlsql-canal is a extension for Byzer, it can be used to parse canal binlog and store it to delta lake.
Notice that DDL is also supported.


## Online Installation

```
!plugin app add "tech.mlsql.plugins.canal.CanalApp" "mlsql-canal-3.3";
```


## Usage

Here is a simple Byzer stream example:

```sql
set streamName="binlog_to_delta";

-- load data from kafka
load kafka.`binlog-canal_test`
options `kafka.bootstrap.servers` = "***"
 and `maxOffsetsPerTrigger`="600000"
as kafka_record;

-- parse canal log
select cast(value as string) as value from kafka_record
as kafka_value;

-- use custom datasource to replay binlog to delta lake
-- Notice that the BinlogToDelta is a custom datasource, you can find it in mlsql-canal-3.3.jar(this extension)
save append kafka_value
as custom.``
options mode = "Append"
and duration = "20"
and sourceTable = "kafka_value"
and checkpointLocation = "checkpoint/binlog_to_delta"
and code = '''
run kafka_value
as BinlogToDelta.``
options dbTable = "canal_test.test";
''';
```