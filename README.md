# Byzer Extensions

This project is a collection of extensions for Byzer.

## Requirements

The user should install the byzer-lang in your local maven repository  before compiling this project.

1. Python >= 3.6
2. Maven >= 3.0

## Byzer-lang dependencies

Clone [byzer-lang](https://github.com/byzer-org/byzer-lang), 
and run the following command to install it in your local maven repository.

```
mvn clean install  -DskipTests -Ponline  -Phive-thrift-server  -Pscala-2.12 
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

