# Byzer-eval

## Introduction

Byzer-eval is an extension of Byzer, which can execute string variable as Byzer script. This means
you can write a script in a variable and execute it.

With the help of UDF of Byzer and Byzer-python, you can build a complex Byzer script 
and then deliver it to this extension to execute it and then get the result table in following steps.

## Online Installation 

```sql
!plugin app add - byzer-eval-3.3;
```

## Offline Installation

## Installation

1. Download the latest release
   from [here](http://store.mlsql.tech/run?action=downloadPlugin&pluginType=MLSQL_PLUGIN&pluginName=byzer-eval-3.3&version=0.1.0-SNAPSHOT).
2. Move the downloaded jar file to the plugin directory of Byzer.
3. Modify `conf/byzer.properties.overwrite` file of Byzer, add the following configurations:

   ```properties
   # Configure entry class of the extension 
   streaming.plugin.clzznames=tech.mlsql.plugins.eval.ByzerEvalApp 
   ```

4. Restart Byzer (./bin/byzer.sh restart) with the new configuration , the extension will be loaded automatically.


## Usage

```sql
set code1='''
select 1 as a as b;
''';

!eval code1;

select * from b as output;
```

## Auth

If you want to control the access of this extension with [byzer-simple-auth](byzer-simple-auth),
try the following command:

```sql
!simpleAuth resource add _ -type mlsql_system -path "__eval__" -allows allwefantasy;
!simpleAuth admin reload;
```



