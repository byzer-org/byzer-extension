# Byzer-objectstore-obs

This plugin is tested with Spark-3.3.0.

## Usage

Assuming obs bucket name is : obs-test, and its region is `cn-southwest-1`. Please change 
`fs.obs.access.key` , `fs.obs.secret.key`, bucket name and region .

```
select 1 as id ;

save overwrite id as FS.`obs://obs-test/id`
where `fs.obs.impl`="org.apache.hadoop.fs.obs.OBSFileSystem"
and `fs.AbstractFileSystem.obs.impl`="org.apache.hadoop.fs.obs.OBSorg.apache.hadoop.fs.obs.OBS"
and `fs.obs.endpoint`="obs.cn-southwest-2.myhuaweicloud.com"
and `fs.obs.access.key`="xxx"
and `fs.obs.secret.key`="xxx"
and `implClass` = "csv";
```

