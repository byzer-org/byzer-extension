# Byzer-objectstore-cos

## Install

```
!plugin app install - "byzer-objectstore-cos-3.3" 
```

## Uninstall

```
!plugin app remove  "byzer-objectstore-cos-3.3" 
```

## Usage

```
load FS.`cosn://xxxx-xxxx/test/a.parquet`
where `fs.cosn.impl`="org.apache.hadoop.fs.CosFileSystem" 
and `fs.AbstractFileSystem.cosn.impl`="org.apache.hadoop.fs.CosN"
and `fs.cosn.bucket.endpoint_suffix`="cos.ap-xxx.myqcloud.com"
and `fs.cosn.userinfo.secretId`="xxxx" 
and `fs.cosn.userinfo.secretKey`="xxxxx" 
and `fs.cosn.tmp.dir`="/tmp/hadoop_cos"  
and implClass="parquet" as output;
```