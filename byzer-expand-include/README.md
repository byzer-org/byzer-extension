## Install command:

```
!plugin script add - byzer-expand-include;
```

## Usage

this plugin allow expand `include*` in `.byzer` files

## Test

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