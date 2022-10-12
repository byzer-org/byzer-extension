# Byzer-objectstore-oss
Aliyun OSS
## Install

```
!plugin app install - "byzer-objectstore-oss-3.3" 
```

## Uninstall

```
!plugin app remove  "byzer-objectstore-oss-3.3" 
```

## Usage

```
include lib.`github.com/allwefantasy/byzer-objectstore`
where alias="objectstore";

include local.`objectstore.oss.mod` 
where ossID = "xxxx" and ossKey="xxxxx";

load FS.`oss://xxxxx-1255000012/test/a.parquet` 
where implClass="parquet" as output;
```