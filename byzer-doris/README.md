# Byzer-Doris

## Introduction
Byzer-doris is an extension for Byzer to read and write [Apache Doris](https://github.com/apache/doris)

## pre-requirements

The doris-spark-connector jar file should be placed in the plugin directory of Byzer 
before this extension is loaded. 

## Online Installation

```sql
!plugin app add - byzer-doris-3.3;
```

## Offline Installation

1. Download the latest release
   from [here](http://store.mlsql.tech/run?action=downloadPlugin&pluginType=MLSQL_PLUGIN&pluginName=byzer-doris-3.3&version=0.1.0-SNAPSHOT).
2. Move the downloaded jar file to the plugin directory of Byzer.
3. Modify `conf/byzer.properties.overwrite` file of Byzer, add the following configurations:

   ```properties
   # Configure entry class of the extension 
   streaming.plugin.clzznames=tech.mlsql.plugins.doris.MLSQLDorisApp 
   ```

4. Restart Byzer (./bin/byzer.sh restart) with the new configuration , the extension will be loaded automatically.


## Usage
To read Doris table, use `load` statement. The following statement reads **Doris db**: zjc_1
**table**: table_hash_1. 

Please note: `doris.fenodes user password` are required
```sql
load doris.`zjc_1.table_hash_1`
and `doris.fenodes`="127.0.0.1:8030"
and `user`="user"
and `password`="xxx"
AS abc;
```

To insert into Doris table, use `save` statement:
```sql
SELECT 11 k1, 11.1 k2 , current_timestamp() dt AS data;

SAVE append data AS doris.`zjc_1.table_hash_1`
WHERE `doris.fenodes`="127.0.0.1:8030"
and `user`="user"
and `password`="xxx";
```
Please note that `overwrite` mode is not supported, `overwrite` is silently changed into
`append` by [spark-doris-connector](https://github.com/apache/doris-spark-connector)

To make your code clean, use `Connect` statement to setup common config. 
The previous examples can be rewritten to:
```sql
CONNECT doris 
WHERE `doris.fenodes`="127.0.0.1:8030"
and `user`="user"
and `password`="xxx"
AS zjc_1;

load doris.`zjc_1.table_hash_1` AS abc;

SELECT 11 k1, 11.1 k2 , current_timestamp() dt AS data;

SAVE append data AS doris.`zjc_1.table_hash_1`;
```

## Build
To build this extension, please follow [spark-doris-connector document](https://doris.apache.org/zh-CN/docs/ecosystem/spark-doris-connector) 
to compile it first. Then run `mvn package install -P shade` to build and install the extension.

## Deploy
Please copy jar file into `${BYZER_HOME}/plugin` .
