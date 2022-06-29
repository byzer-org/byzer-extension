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
      val res = et.train(seq_df1, "", Map("atRound" -> "2"))
      res.show()
      val r0 = res.collectAsList().get(0).toSeq
      println(r0.mkString(","))
      assert(r0.mkString(",") === "income1,433000.00,1.00,0.17,0.00,453000.00,5.00,44726.95,18259.70,533000.00,432000.00,0.00,0.00,0,8,double,1,433000.0,433000.0,434000.0")
      val r1 = res.collectAsList().get(1).toSeq
      println(r1.mkString(","))
      assert(r1.mkString(",") === "name,,1.0,0.0,0.17,0,6,,,,,5,0,1,-2,string,2,0.0,0.0,0.0")
      val r2 = res.collectAsList().get(2).toSeq
      println(r2.mkString(","))
      assert(r2.mkString(",") === "age,40.00,0.17,0.00,0.00,34.67,6.00,17.77,7.26,57.00,10.00,0.00,0.00,1,4,integer,3,21.0,30.0,50.0")
      val r3 = res.collectAsList().get(3).toSeq
      println(r3.mkString(","))
      assert(r3.mkString(",") === "income,,0.83,0.0,0.17,0,6,,,,,6,0,0,-2,string,4,0.0,0.0,0.0")
    }
  }
}