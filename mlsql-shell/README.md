# mlsql-shell

## Introduction

This plugin provide the ability to execute shell command in MLSQL.
Notice that it is not a secure way to execute shell command in MLSQL, so please use it carefully.

## Online Installation

```sql
!plugin app add - byzer-shell-3.3;
```

## Offline Installation

1. Download the latest release
   from [here](http://store.mlsql.tech/run?action=downloadPlugin&pluginType=MLSQL_PLUGIN&pluginName=mlsql-shell-3.3&version=0.1.0-SNAPSHOT).
2. Move the downloaded jar file to the plugin directory of Byzer.
3. Modify `conf/byzer.properties.overwrite` file of Byzer, add the following configurations:

   ```properties
   # Configure entry class of the extension 
   streaming.plugin.clzznames=tech.mlsql.plugins.shell.app.MLSQLShell 
   ```

4. Restart Byzer (./bin/byzer.sh restart) with the new configuration , the extension will be loaded automatically.

## Usage

```sql
!sh curl -XGET "https://www.byzer.org";
```

## Auth 


If you want to control the access of this extension with [byzer-simple-auth](byzer-simple-auth),
try the following command:

```sql
!simpleAuth resource add _ -type mlsql_system -path "__mlsql_shell__" -allows allwefantasy;
!simpleAuth admin reload;
```

In this extension, there is a function called `!copyFromLocal` which can copy file from local to HDFS/Object Storage.
Try the following command to add the permission:

```sql
!simpleAuth resource add _ -type mlsql_system -path "__copy_from_local__" -allows allwefantasy;
!simpleAuth admin reload;
```











