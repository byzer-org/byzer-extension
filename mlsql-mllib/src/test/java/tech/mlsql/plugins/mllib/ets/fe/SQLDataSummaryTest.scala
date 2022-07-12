package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.test.BasicMLSQLConfig
import org.scalatest.FunSuite

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/6/27 19:07
 *
 */
class SQLDataSummaryTest extends FlatSpec with SparkOperationUtil with Matchers with BasicMLSQLConfig with BeforeAndAfterAll {
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

  "DataSummary" should "Summarize the Dataset" in {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val et = new SQLDataSummary()
      val sseq = Seq(
        ("elena", 57, 110L, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 110F),
        ("abe", 50, 120L, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 120F),
        ("AA", 10, 130L, "432000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 130F),
        ("cc", 40, 140L, "", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 140F),
        ("", 30, 150L, "434000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 150F),
        ("bb", 21, 160L, "533000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 160F)
      )
      var seq_df = spark.createDataFrame(sseq).toDF("name", "age", "mock_col1", "income", "date", "mock_col2")
      var seq_df1 = seq_df.select(seq_df("income").cast(DoubleType).alias("income1"), col("*"))
      //      seq_df1 = seq_df1.select(col("name"))
      val res = et.train(seq_df1, "", Map("atRound" -> "2"))
      res.show()
      val sseq2 = Seq(
        (336, 123, "plan1", "", "534", 1, Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 1, Double.NaN),
        (336, 123, "plan1", "", "534", 1, Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 1, Double.NaN),
        (336, 123, "plan1", "", "534", 1, Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 1, Double.NaN)
      )
      var seq_df2 = spark.createDataFrame(sseq2).toDF("id", "dataset_id", "leftPlan_name", "right_plan_name", "plan_desc", "user_id", "ctime", "mtime", "alived", "plan_name_convert")
      val res1 = et.train(seq_df2, "", Map("atRound" -> "2"))
      res1.show()
      val r0 = res.collectAsList().get(0).toSeq
      println(r0.mkString(","))
      assert(r0.mkString(",") === "income1,433000.0,0.67,0.17,0.0,453000.0,5,44726.95,18259.7,533000.0,432000.0,0,0,0,8,double,1,0.0,432000.0,533000.0")
      val r1 = res.collectAsList().get(1).toSeq
      println(r1.mkString(","))
      assert(r1.mkString(",") === "name,,1.0,0.0,0.17,,6,,,,,5,0,1,,string,2,0.0,0.0,0.0")
      val r2 = res.collectAsList().get(2).toSeq
      println(r2.mkString(","))
      assert(r2.mkString(",") === "age,40,1.0,0.0,0.0,34.67,6,17.77,7.26,57,10,0,0,1,4,integer,3,10.0,21.0,57.0")
      val r3 = res.collectAsList().get(3).toSeq
      println(r3.mkString(","))
      assert(r3.mkString(",") === "mock_col1,130,1.0,0.0,0.0,135.0,6,18.71,7.64,160,110,0,0,1,8,long,4,110.0,120.0,160.0")
      val r4 = res.collectAsList().get(4).toSeq
      println(r4.mkString(","))
      assert(r4.mkString(",") === "income,,0.83,0.0,0.17,,6,,,,,6,0,0,,string,5,0.0,0.0,0.0")
      val r5 = res.collectAsList().get(5).toSeq
      println(r5.mkString(","))
      assert(r5.mkString(",") === "date,,0.17,0.0,0.0,,6,,,,,0,0,0,8,timestamp,6,0.0,0.0,0.0")
      val r6 = res.collectAsList().get(6).toSeq
      println(r6.mkString(","))
      assert(r6.mkString(",") === "mock_col2,110.0,1.0,0.0,0.0,135.0,6,18.71,7.64,160.0,110.0,0,0,1,4,float,7,110.0,120.0,160.0")
    }
  }
}