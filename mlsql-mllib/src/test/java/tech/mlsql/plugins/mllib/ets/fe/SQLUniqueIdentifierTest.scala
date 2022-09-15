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
class SQLUniqueIdentifierTest extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig with BeforeAndAfterAll with BeforeAndAfterEach {
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

  test("SQLUniqueIdentifier should Returns the unique id of a field in a table") {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val et = new SQLUniqueIdentifier()
      // Does not support char type
      var sseq = Seq(
        ("ab_c_1", "2022-6-7 10:37", "http://lucy.asdfasdf.sadfa.asdfa", "a", "aaa1_13", "2022-6-7", "1,2,3,4", 57.12, "  A B  ", "15552231521", 433000, 0),
        ("ab_c_2", "2022-6-7 10:37", "http://lucy.asdfasdf.sadfa.asdfa", "a", "a@qe#", "2022-6-7", "1,2,3,4", 67.12, "  A B  ", "15552231521", 1200, 0),
        ("ab_c_3", "2022-6-7 10:37", "http://lucy.asdfasdf.sadfa.asdfa", "a", "c_dsar@", "2022-6-7", "1,2,3,4", 57.12, "  A B  ", "15552231521", 89000, 0),
        ("ab_c_4", "2022-6-7 10:37", "http://lucy.asdfasdf.sadfa.asdfa", "a", "ne&ew", "2022-6-7", "1,2,3,4", 25.12, "  A B  ", "15552231521", 36000, 1),
        ("ab_c_5", "2022-6-7 10:37", "http://lucy.asdfasdf.sadfa.asdfa", "a", "e", "2022-6-7", "1,2,3,4", 31.12, "  A B  ", "15552231521", 300000, 1),
        ("ab_c_6", "2022-6-7 10:37", "http://lucy.asdfasdf.sadfa/asdfa", "a", "def$$$ewe", "2022-6-7", "1,2,3,4", 23.12, "  A B  ", "15552231521", 238000, 1)
      )
      val seq_df = spark.createDataFrame(sseq).toDF("name", "ctime", "harbor_url", "convert_char", "convert_char1", "convert_date", "rbt", "age", "replace_sp", "phone", "income", "label")
      seq_df.show()
      val seq_df1 = seq_df.select(col("*"))
      val res = et.train(seq_df1, "", Map("source" -> "new", "columnName" -> "id"))
      res.show()

      val r0 = res.collectAsList().get(0).toSeq
      println(r0.mkString(","))
      assert(r0.mkString(",") === "1,ab_c_1,2022-6-7 10:37,http://lucy.asdfasdf.sadfa.asdfa,a,aaa1_13,2022-6-7,1,2,3,4,57.12,  A B  ,15552231521,433000,0")
      val r1 = res.collectAsList().get(1).toSeq
      println(r1.mkString(","))
      assert(r1.mkString(",") === "2,ab_c_2,2022-6-7 10:37,http://lucy.asdfasdf.sadfa.asdfa,a,a@qe#,2022-6-7,1,2,3,4,67.12,  A B  ,15552231521,1200,0")
      val r2 = res.collectAsList().get(2).toSeq
      println(r2.mkString(","))
      assert(r2.mkString(",") === "3,ab_c_3,2022-6-7 10:37,http://lucy.asdfasdf.sadfa.asdfa,a,c_dsar@,2022-6-7,1,2,3,4,57.12,  A B  ,15552231521,89000,0")
      val r3 = res.collectAsList().get(3).toSeq
      println(r3.mkString(","))
      assert(r3.mkString(",") === "4,ab_c_4,2022-6-7 10:37,http://lucy.asdfasdf.sadfa.asdfa,a,ne&ew,2022-6-7,1,2,3,4,25.12,  A B  ,15552231521,36000,1")
      val r4 = res.collectAsList().get(4).toSeq
      println(r4.mkString(","))
      assert(r4.mkString(",") === "5,ab_c_5,2022-6-7 10:37,http://lucy.asdfasdf.sadfa.asdfa,a,e,2022-6-7,1,2,3,4,31.12,  A B  ,15552231521,300000,1")
    }
  }

  test("param of startIndex should Returns the column starting at the specified index") {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val et = new SQLUniqueIdentifier()
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
      val res = et.train(seq_df1, "", Map("source" -> "new", "columnName" -> "id", "startIndex" -> "100"))
      res.show()
      val r0 = res.collectAsList().get(0).toSeq
      println(r0.mkString(","))
      assert(r0.mkString(",") === "100,433000.0,elena,57,433000,2021-03-08 18:00:00.0")
      val r1 = res.collectAsList().get(1).toSeq
      println(r1.mkString(","))
      assert(r1.mkString(",") === "101,433000.0,abe,50,433000,2021-03-08 18:00:00.0")
      val r2 = res.collectAsList().get(2).toSeq
      println(r2.mkString(","))
      assert(r2.mkString(",") === "102,432000.0,AA,10,432000,2021-03-08 18:00:00.0")
      val r3 = res.collectAsList().get(3).toSeq
      println(r3.mkString(","))
      assert(r3.mkString(",") === "103,null,cc,40,,2021-03-08 18:00:00.0")
      val r4 = res.collectAsList().get(4).toSeq
      println(r4.mkString(","))
      assert(r4.mkString(",") === "104,434000.0,,30,434000,2021-03-08 18:00:00.0")
    }
  }

}