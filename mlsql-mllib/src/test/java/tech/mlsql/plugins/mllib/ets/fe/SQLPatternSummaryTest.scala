package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tech.mlsql.test.BasicMLSQLConfig
import org.apache.spark.ml.fpm.FPGrowth
import streaming.core.strategy.platform.SparkRuntime

import java.util.UUID

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/7/13 12:48
 *
 */
class SQLPatternSummaryTest extends FlatSpec with SparkOperationUtil with Matchers with BasicMLSQLConfig with BeforeAndAfterAll {

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
      val et = new SQLPatternDistribution()
      val sseq = Seq(
        ("elenA我的", 57, 110L, "433000", 110F),
        ("我哎abe", 50, 120L, "433000", 120F),
        ("AA", 10, 130L, "432000", 130F),
        ("cc", 40, 140L, "", 140F),
        ("", 30, 150L, "434000", 150F),
        ("bb", 21, 160L, "533000", 160F),
        ("Abc.中文字符#123.01/abc中文", 22, 160L, "220", 170F),
        ("AAAAAA,A,AA", 22, 160L, "220", 170F)
      )
      var seq_df = spark.createDataFrame(sseq).toDF("name", "age", "mock_col1", "income", "mock_col2")
      var res = et.train(seq_df, "", Map())
      res.show()
    }
  }

}
