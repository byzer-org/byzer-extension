## Install command:
```
!plugin ds add - "delta-enhancer-3.3";
```

Install as App:
```
!plugin app add "tech.mlsql.plugins.delta.app.ByzerDelta" "delta-enhancer-3.3";
```

### Demo Sql
create a delta table
```sql
set rawText='''
{"id":1,"content":"MLSQL 是一个好的语言","label":0.0},
{"id":2,"content":"Spark 是一个好的语言","label":1.0}
{"id":3,"content":"MLSQL 语言","label":0.0}
{"id":4,"content":"MLSQL 是一个好的语言","label":0.0}
{"id":5,"content":"MLSQL 是一个好的语言","label":1.0}
{"id":6,"content":"MLSQL 是一个好的语言","label":0.0}
{"id":7,"content":"MLSQL 是一个好的语言","label":0.0}
{"id":8,"content":"MLSQL 是一个好的语言","label":1.0}
{"id":9,"content":"Spark 好的语言","label":0.0}
{"id":10,"content":"MLSQL 是一个好的语言","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

save append orginal_text_corpus as delta.`test_demo.table1`;
```

###  Usage

#### Describe Delta Table `!delta desc {schema.tableName}`
```sql
!deltaTable desc test_demo.table1;
```

#### Truncate Table `!delta truncate {schema.tableName}`
```sql
!deltaTable truncate "test_demo.table1";
```

#### Delete By Condition `!delta delete {schema.tableName} {condition}`
删除id > 1的数据
```sql
!deltaTable delete test_demo.table1 'id > 1';
```

#### Remove Table `!delta remove {schema.tableName}`
```sql
!deltaTable remove "test_demo.table1";
```

#### Clear Table History `!delta vacuum {schema.tableName} {retentionHours}`
```sql
-- 删除168个小时之前的历史版本
!deltaTable vacuum test_demo.table1 168;
```

