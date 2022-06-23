package tech.mlsql.test

import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.plugins.ets.EmptyTable

/**
 * 23/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ETTest extends FlatSpec with SparkOperationUtil with Matchers with BasicMLSQLConfig with BeforeAndAfterAll {
  "et" should "test-example" in {
    withBatchContext(setupBatchContext(batchParams)) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val et = new EmptyTable()
      val df = spark.emptyDataFrame
      val newDf = et.train(df, "", Map())
      newDf.show()

    }
  }
}
