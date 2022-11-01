package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.plugins.ds.app.Xtreme1Format
import tech.mlsql.test.BasicMLSQLConfig

import java.net.URI
import java.util.UUID


class Xtreme1DSTest extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig with BeforeAndAfterAll with BeforeAndAfterEach {
  def startParams = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-spark.mlsql.path.schemas", "oss,s3a,file"
  )

  test("xtreme1") {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val session: SparkSession = runtime.sparkSession
      autoGenerateContext(runtime)
      val jsonPath = "/Users/allwefantasy/Downloads/balloon-20221027122838.json"
      val jsonFileStr = session.read.format("text").option("wholeFile", "true").
        load(jsonPath).collect().head.getString(0) //HDFSOperatorV2.readFile(jsonPath)
      val xtreme1Format = JSONTool.parseJson[Xtreme1Format](jsonFileStr)

      val rows = xtreme1Format.contents.map(item =>
        Row.fromSeq(Seq(item.data.image.split("/").last.split("\\?").head, JSONTool.toJsonStr(item))))

      val p = new URI(jsonPath)
      val withSchema = p.getScheme != null
      val metaDF = session.createDataFrame(session.sparkContext.parallelize(rows), StructType(Seq(StructField("name", StringType), StructField("meta", StringType))))


    }
  }
}