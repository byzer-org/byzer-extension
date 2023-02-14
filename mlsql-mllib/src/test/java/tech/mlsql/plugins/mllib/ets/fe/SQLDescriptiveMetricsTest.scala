package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.test.BasicMLSQLConfig

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID

/**
 * 12/07/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class SQLDescriptiveMetricsTest extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig with BeforeAndAfterAll with BeforeAndAfterEach {
  def startParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  test("DescriptiveMetrics should Returns the frequency of a field in a table") {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val et = new SQLDescriptiveMetrics()
      val sseq = Seq(
        ("elena", 57, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("abe", 50, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("AA", 10, "432000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("cc", 40, "", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("", 30, "434000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("bb", 21, "533000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)))
      )
      val seq_df = spark.createDataFrame(sseq).toDF("name", "age", "income", "date")
      seq_df.show()
      val seq_df1 = seq_df.select(seq_df("income").cast(DoubleType).alias("income1"), col("*"))
      val res = et.train(seq_df1, "", Map("metricSize" -> "100"))
      res.show()
      val r0 = res.collectAsList().get(0).toSeq
      println(r0.mkString(","))
      assert(r0.mkString(",") === "income1,[{\"432000.0\" : 1}, {\"433000.0\" : 2}, {\"434000.0\" : 1}, {\"533000.0\" : 1}]")
      val r1 = res.collectAsList().get(1).toSeq
      println(r1.mkString(","))
      assert(r1.mkString(",") === "name,[{\"AA\" : 1}, {\"abe\" : 1}, {\"bb\" : 1}, {\"cc\" : 1}, {\"elena\" : 1}]")
      val r2 = res.collectAsList().get(2).toSeq
      println(r2.mkString(","))
      assert(r2.mkString(",") === "age,[{\"10\" : 1}, {\"21\" : 1}, {\"30\" : 1}, {\"40\" : 1}, {\"50\" : 1}, {\"57\" : 1}]")
      val r3 = res.collectAsList().get(3).toSeq
      println(r3.mkString(","))
      assert(r3.mkString(",") === "income,[{\"432000\" : 1}, {\"433000\" : 2}, {\"434000\" : 1}, {\"533000\" : 1}]")
      val r4 = res.collectAsList().get(4).toSeq
      println(r4.mkString(","))
      assert(r4.mkString(",") === "date,[{\"2021-03-08 18:00:00.0\" : 6}]")
    }
  }
}