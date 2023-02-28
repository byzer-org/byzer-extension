# Byzer-expand-include Extension

Byzer-expand-include is a Byzer extension that allow expanding `include*` in `.byzer` files

## Online Installation

```
!plugin app add - byzer-expand-include-3.3;
```

## Offline Installation

1. Download the latest version of the extension from [here](http://store.mlsql.tech/run?action=downloadPlugin&pluginType=MLSQL_PLUGIN&pluginName=byzer-expand-include-3.3&version=0.1.0-SNAPSHOT)
2. Move the downloaded jar file to the plugin directory of Byzer.
3. Modify `conf/byzer.properties.overwrite` file of Byzer, add the following configurations:

    ```properties
    # Configure entry class of the extension 
    streaming.plugin.clzznames=tech.mlsql.plugins.expandinclude.MLSQLExpandInclude    
    ```
4. Restart Byzer (./bin/byzer.sh restart) with the new configuration , the extension will be loaded automatically.

## Usage

```shell
curl --location --request POST 'http://localhost:9004/run/script?executeMode=expandInclude' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--data-urlencode 'owner=admin' \
--data-urlencode 'sql=include lib.`gitee.com/allwefantasy/lib-core`
where alias="libCore";
include local.`libCore.udf.hello`;
select hello() as name as output;'
```

## result
```json
{
    "success": true,
    "sql": "run command as EmptyTable.``;set format='''local''';register ScriptUDF.`` as hello where \nlang=\"scala\"\nand code='''\n    def apply()={\n        \"hello world\"\n    }\n'''\nand udfType=\"udf\";select hello() as name as output;"
}
```