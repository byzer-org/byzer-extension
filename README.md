# Byzer Extensions

This project is a collection of extensions for Byzer.

## Requirements

The user should install the byzer-lang in your local maven repository before compiling this project.

1. Python >= 3.6
2. Maven >= 3.0

## Byzer-lang dependencies

Clone [byzer-lang](https://github.com/byzer-org/byzer-lang),
and run the following command to install it in your local maven repository.

```
mvn clean install -DskipTests -Ponline -pl streamingpro-mlsql -am  
```

## Install build tools

[mlsql_plugin_tool](https://github.com/allwefantasy/mlsql_plugin_tool) is a tool to build tool for Byzer extensions.

Installing command:

```
pip install mlsql_plugin_tool
```

Now you can try the following command to check if the tool is installed successfully.

```
mlsql_plugin_tool build --module_name xxxxx --spark spark330
```

1. spark:  spark243, spark311 and spark330.
2. module_name e.g mlsql-excel, byzer-simple-auth

Once build success, the system will show message like fowllowing:

```

====Build success!=====
 File location 0：
 /Users/allwefantasy/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/mlsqlplugins/xxx/target/xxx-0.1.0-SNAPSHOT.jar

```

## Extensions list

| Extension                                          | Description                                                                          |
|----------------------------------------------------|--------------------------------------------------------------------------------------|
| [byzer-simple-auth](byzer-simple-auth)             | A simple auth extension for Byzer.                                                   |
| [byzer-yaml-visulization](byzer-yaml-visulization) | A yaml visulization extension for Byzer.                                             |
| [byzer-eval](byzer-eval)                           | Byzer-eval is an extension which can execute string variable as Byzer script.        |
| [byzer-doris](byzer-doris)                         | Doris datasource extension for Byzer.                                                |
| [byzer-expand-include](byzer-expand-include)       | Expand include extension for Byzer.                                                  |
| [byzer-objectstore-blob](byzer-objectstore-blob)   | Azure Object Storage                                                                 |
| [byzer-objectstore-cos](byzer-objectstore-cos)     | Tencent Cloud Object Storage                                                         |
| [byzer-objectstore-s3](byzer-objectstore-s3)       | Amazon S3 Object Storage                                                             |
| [byzer-objectstore-oss](byzer-objectstore-oss)     | Alibaba Cloud Object Storage                                                         |
| [byzer-objectstore-obs](byzer-objectstore-obs)     | Huawei Cloud Object Storage                                                          |
| [byzer-openmldb](byzer-openmldb)                   | OpenMLDB extension for Byzer.                                                        |
| [byzer-xgboost](byzer-xgboost)                     | XGBoost extension for Byzer.                                                         |
| [connect-persist](connect-persist)                 | Persist the connect statement to delta lake                                          |
| [delta-enhancer](delta-enhancer)                   | Delta lake tools  for Byzer.                                                         |
| [last-command](last-command)                       | Get the result of last statement. This is useful for reference the output of command |
| [mlsql-excel](mlsql-excel)                         | Excel datasource extension for Byzer.                                                |
| [mlsql-assert](mlsql-assert)                       | Assert function support for Byzer.                                                   |
| [mlsql-canal](mlsql-canal)                         | Replay the CDC which is generated from canal to Delta Lake                           |
| [mlsql-shell](mlsql-shell)                         | Execute shell command in Byzer.                                                      |
| [mlsql-ke](mlsql-ke)                               | A tool to operate Kyligence Enterprise.                                              |
| [stream-persist](stream-persist)                   | Persist the stream job to delta lake for recovery                                    |
| [table-repartition](table-repartition)             | Repartition the table for better performance.                                        |
| [mlsql-mllib](mlsql-mllib)                         | Some statistical extensions for Byzer.                                               |
| [mlsql-ext-ets](mlsql-ext-ets)                     | Some extension collection for Byzer.                                                 |
| [mlsql-ds](mlsql-ds)                               | Some datasource  collection for Byzer.                                               |
| [echo-controller](echo-controller)                 | A simple demo extension for new http request.                                        |


