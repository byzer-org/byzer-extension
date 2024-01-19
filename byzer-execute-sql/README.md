<H1 align="center">
MPP-JDBC Extension for Byzer-SQL
</H1>

<h3 align="center">
Make MPP easy to use by Byzer-SQL
</h3>

<p align="center">
| <a href="#"><b>Documentation</b></a> | <a href="#"><b>Blog</b></a> | | <a href="#"><b>Discord</b></a> |

</p>

---

*Latest News* 🔥

- [2024/01] Release MPP-JDBC Extension [0.1.1]((https://download.byzer.org/byzer-extensions/nightly-build/byzer-execute-sql-3.3_2.12-0.1.1.jar)) for Byzer-SQL
- Old Version: [0.1.0](https://download.byzer.org/byzer-extensions/nightly-build/byzer-execute-sql-3.3_2.12-0.1.0-SNAPSHOT.jar) 


---

This is the MPP-JDBC Extension for Byzer-SQL, which is make Byzer-SQL can connect to MPP by JDBC, and map MPP's table to Byzer-SQL's table.

---

* [Versions](#Versions)
* [Installation](#Installation)
* [Quick Start](#Quick-Start)
* [Contributing](#Contributing)

---

## Versions
- [0.1.1](https://download.byzer.org/byzer-extensions/nightly-build/)： Use parquet as the default file format

---

## Installation

1. Download the latest version from [here](https://download.byzer.org/byzer-extensions/nightly-build/).
2. Copy the jar file to the `plugin` folder of Byzer-SQL.
3. Add the following configuration to the `conf/byzer.properties.override` file of Byzer-SQL:

```properties
streaming.plugin.clzznames=tech.mlsql.plugins.execsql.ExecSQLApp,[YOUR_OTHER_PLUGINS_LIST]
```

---

## Quick Start

Create a connection to a MySQL database:

```sql
!conn business
"url=jdbc:mysql://127.0.0.1:3306/business?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&useSSL=false"
"driver=com.mysql.jdbc.Driver"
"user=root"
"password=xxxx";
```

`business` is the name of the connection, you can use it to execute SQL later.
The other parameters are the same as JDBC.

Remove a connection:

```sql

!conn remove business;
``` 

Notice that the connection should be removed manually, otherwise may cause connection leak, and the 
DataSource will refuse to create new connection.

Create temporary table  in SQL:
      
```sql
!exec_sql '''

create table1 from select content as instruction,"" as input,summary as output
from tunningData

''' by business;
```


or you can select data from a table and map it to a Byzer-SQL table:

```sql
!exec_sql tunningData  from '''

select content as instruction,"" as input,summary as output
from tunningData

''' by business;

select * from tunningData as output;
```

The data will be stored as temporary file(parquet) and then be mapped to a Byzer-SQL table `tunningData`.
