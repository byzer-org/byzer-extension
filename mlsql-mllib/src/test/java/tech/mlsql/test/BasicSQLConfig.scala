package tech.mlsql.test

import java.util.UUID

/**
 * Created by allwefantasy on 6/5/2018.
 */
trait BasicMLSQLConfig {
  def batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  def batchParamsWithoutHive = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )



  def batchParamsWithJsonFile(name: String) = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.job.file.path", s"classpath:///test/${name}.json"
  )


  def batchParamsWithAPI = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.deploy.rest.api", "true"
  )

  def batchParamsWithCarbondata = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.enableCarbonDataSupport", "true",
    "-streaming.carbondata.store", "/tmp/carbondata/store",
    "-streaming.carbondata.meta", "/tmp/carbondata/meta"
  )

  def mlsqlParams = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )
}
