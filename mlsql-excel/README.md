## Install

```
!plugin ds add - "mlsql-excel-3.3";
```

or install as app:

```
!plugin app add "tech.mlsql.plugins.ds.MLSQLExcelApp" "mlsql-excel-3.3";
```


## Usage

```sql
load excel.`/tmp/upload/example_en.xlsx` 
where useHeader="true" and 
maxRowsInMemory="100" 
and dataAddress="A1:C8"
as data;

select * from data as output;
```





