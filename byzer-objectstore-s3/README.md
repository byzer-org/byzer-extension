# S3 插件

## 部署

### 自动化部署

自动化部署指热加载 S3 插件，无需重启 Byzer-lang 进程。请执行以下 Byzer 代码:

```sql
!plugin app add - "byzer-objectstore-s3-3.3";
```
Byzer-lang 将下载 S3 插件 jar 包，保存在数据湖。执行成功例子如下：

| pluginName               | path                                                                                                                      |pluginType |version|
|--------------------------|---------------------------------------------------------------------------------------------------------------------------|---|---|
| byzer-objectstore-s3-3.3 | /byzer-lang/delta/__instances__/byzer-lang/__mlsql__/files/store/plugins/byzer-objectstore-s3-3.3_2.12-0.1.0-SNAPSHOT.jar | app | 0.1.0-SNAPSHOT |

S3 插件约 250 兆，下载耗时较长。请耐心等待。您也可以手动部署。

您可以执行 `!plugin list; ` Byzer 代码，以检查自动安装的插件。结果不包含手动部署的插件。

### 手动部署
请执行 Maven 命令，编译 S3 jar 包，生成 S3 插件 jar 包， 位于 `byzer-objectstore-s3/target/byzer-objectstore-s3-3.3_2.12-0.1.0-SNAPSHOT.jar` 。

```shell
## 在 byzer-objectstore-s3 目录，请执行
mvn package -DskipTest -P shade

## 在项目根目录，请执行
mvn package -DskipTest -P shade -pl byzer-objectstore-s3
```

拷贝 jar 至 您部署的 Byzer-lang 的 plugin 子目录。执行 `bin/byzer.sh start` 启动 Byzer-lang 进程。`byzer.sh` 自动添加 plugin 所有 jar 至 Byzer-lang classpath，
您无需手动添加。

