package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.test.BasicMLSQLConfig

import java.io.{File, IOException}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.{Date, UUID}

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/6/27 19:07
 *
 */
class SQLDataSummaryV2Test extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig with BeforeAndAfterAll {
  val allMetrics: String = "columnName,ordinalPosition,%25,%75,blankValueRatio,dataLength,dataType,max,maximumLength,mean,median,min,minimumLength," +
    "nonNullCount,nullValueRatio,skewness,totalCount,standardDeviation,standardError,uniqueValueRatio,categoryCount,primaryKeyCandidate,mode"


  def startParams = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-spark.sql.shuffle.partitions", "12",
    "-spark.default.parallelism", "12",
    "-spark.executor.memoryOverheadFactor", "0.2",
    "-spark.driver.maxResultSize", "3g"
  )

  /**
   * Get the byzer streamingpro-it absolute path. e.g.: /opt/project/byzer-lang/streamingpro-it/
   *
   * @return
   */
  def getCurProjectRootPath(): String = {
    var base: String = null
    try {
      val testClassPath: String = classOf[SQLDataSummaryV2Test].getResource(File.separator).getPath
      val directory: File = new File(testClassPath + ".." + File.separator + ".." + File.separator)
      base = directory.getCanonicalPath + File.separator
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    if (base == null) {
      base = "." + File.separator
    }
    base
  }

  test("DataSummary should summarize the Dataset") {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark: SparkSession = runtime.sparkSession
      val et = new SQLDataSummaryV2()
      val sseq1 = Seq(
        ("elena", 57, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("abe", 50, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("AA", 10, "432000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("cc", 40, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("", 30, "434000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0))),
        ("bb", 21, "533000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)))
      )
      val seq_df1 = spark.createDataFrame(sseq1).toDF("name", "age", "income", "date")
      val res1DF = et.train(seq_df1, "", Map("atRound" -> "2",
        "metrics" -> allMetrics
      ))
      res1DF.show()
      println(res1DF.collect()(0).mkString(","))
      println(res1DF.collect()(1).mkString(","))
      println(res1DF.collect()(2).mkString(","))
      println(res1DF.collect()(3).mkString(","))
      assert(res1DF.collect()(0).mkString(",") === "name,1,,,,0.1667,5,string,elena,5,,AA,0,5,0.0,,6,,,1.0,5,1,")
      assert(res1DF.collect()(1).mkString(",") === "age,2,23.25,35.0,47.5,0.0,4,integer,57.0,,34.67,10.0,,6,0.0,-0.11,6,17.77,7.26,1.0,0,1,")
      assert(res1DF.collect()(2).mkString(",") === "income,3,,,,0.0,6,string,533000.0,6,,432000.0,6,6,0.0,,6,,,0.6667,4,0,433000.0")
      assert(res1DF.collect()(3).mkString(",") === "date,4,,,,0.0,8,timestamp,2021-03-08 18:00:00,,,2021-03-08 18:00:00,,6,0.0,,6,,,0.1667,1,0,2021-03-08 18:00:00")

      val sseq = Seq(
        ("elena", 57, 57, 110L, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 110F, true, null, null, BigDecimal.valueOf(12), 1.123D),
        ("abe", 57, 50, 120L, "433000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 120F, true, null, null, BigDecimal.valueOf(2), 1.123D),
        ("AA", 57, 10, 130L, "432000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 130F, true, null, null, BigDecimal.valueOf(2), 2.224D),
        ("cc", 0, 40, 100L, "", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), Float.NaN, true, null, null, BigDecimal.valueOf(2), 2D),
        ("", -1, 30, 150L, "434000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), 150F, true, null, null, BigDecimal.valueOf(2), 3.375D),
        ("bb", 57, 21, 160L, "533000", Timestamp.valueOf(LocalDateTime.of(2021, 3, 8, 18, 0)), Float.NaN, false, null, null, BigDecimal.valueOf(2), 3.375D)
      )
      val seq_df = spark.createDataFrame(sseq).toDF("name", "favoriteNumber", "age", "mock_col1", "income", "date", "mock_col2", "alived", "extra", "extra1", "extra2", "extra3")
      val res2DF = et.train(seq_df, "", Map("atRound" -> "2", "metrics" -> allMetrics))
      res2DF.show()
      println(res2DF.collect()(0).mkString(","))
      println(res2DF.collect()(1).mkString(","))
      println(res2DF.collect()(2).mkString(","))
      println(res2DF.collect()(3).mkString(","))
      println(res2DF.collect()(4).mkString(","))
      println(res2DF.collect()(5).mkString(","))
      println(res2DF.collect()(6).mkString(","))
      println(res2DF.collect()(7).mkString(","))
      println(res2DF.collect()(8).mkString(","))
      println(res2DF.collect()(9).mkString(","))
      println(res2DF.collect()(10).mkString(","))
      println(res2DF.collect()(11).mkString(","))
      assert(res2DF.collect()(0).mkString(",") === "name,1,,,,0.1667,5,string,elena,5,,AA,0,5,0.0,,6,,,1.0,5,1,")
      assert(res2DF.collect()(1).mkString(",") === "favoriteNumber,2,14.25,57.0,57.0,0.0,4,integer,57.0,,37.83,-1.0,,6,0.0,-0.71,6,29.69,12.12,0.5,0,0,57.0")
      assert(res2DF.collect()(2).mkString(",") === "age,3,23.25,35.0,47.5,0.0,4,integer,57.0,,34.67,10.0,,6,0.0,-0.11,6,17.77,7.26,1.0,0,1,")
      assert(res2DF.collect()(3).mkString(",") === "mock_col1,4,112.5,125.0,145.0,0.0,8,long,160.0,,128.33,100.0,,6,0.0,0.22,6,23.17,9.46,1.0,0,1,")
      assert(res2DF.collect()(4).mkString(",") === "income,5,,,,0.1667,6,string,533000.0,6,,432000.0,0,5,0.0,,6,,,0.8,4,0,433000.0")
      assert(res2DF.collect()(5).mkString(",") === "date,6,,,,0.0,8,timestamp,2021-03-08 18:00:00,,,2021-03-08 18:00:00,,6,0.0,,6,,,0.1667,1,0,2021-03-08 18:00:00")
      assert(res2DF.collect()(6).mkString(",") === "mock_col2,7,117.5,125.0,135.0,0.0,4,float,150.0,,127.5,110.0,,4,0.3333,0.43,6,17.08,8.54,1.0,0,1,")
      assert(res2DF.collect()(7).mkString(",") === "alived,8,,,,0.0,1,boolean,true,,,false,,6,0.0,,6,,,0.3333,2,0,true")
      assert(res2DF.collect()(8).mkString(",") === "extra,9,,,,0.0,,void,,,,,,0,1.0,,6,,,0.0,0,0,")
      assert(res2DF.collect()(9).mkString(",") === "extra1,10,,,,0.0,,void,,,,,,0,1.0,,6,,,0.0,0,0,")
      assert(res2DF.collect()(10).mkString(",") === "extra2,11,2.0,2.0,2.0,0.0,16,decimal(38,18),12.0,,3.67,2.0,,6,0.0,1.79,6,4.08,1.67,0.3333,0,0,2.0")
      assert(res2DF.collect()(11).mkString(",") === "extra3,12,1.34,2.11,3.09,0.0,8,double,3.38,,2.2,1.12,,6,0.0,0.15,6,1.01,0.41,0.6667,0,0,")

      val sseq2: Seq[(Null, Null)] = Seq(
        (null, null),
        (null, null)
      )
      val seq_df2 = spark.createDataFrame(sseq2).toDF("col1", "col2")
      val res3DF = et.train(seq_df2, "", Map("atRound" -> "2",
        "metrics" -> allMetrics
      ))
      res3DF.show()
      println("-----------res3DF-----------------")
      println(res3DF.collect()(0).mkString(","))
      println(res3DF.collect()(1).mkString(","))
      assert(res3DF.collect()(0).mkString(",") === "col1,1,,,,0.0,,void,,,,,,0,1.0,,2,,,0.0,0,0,")
      assert(res3DF.collect()(1).mkString(",") === "col2,2,,,,0.0,,void,,,,,,0,1.0,,2,,,0.0,0,0,")

      val colNames = Array("id", "name", "age", "birth")
      val schema = StructType(colNames.map(fieldName => StructField(fieldName, StringType, nullable = true)))
      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      val res4DF: DataFrame = et.train(emptyDf, "", Map("atRound" -> "2"))
      println("-----------res4DF start-----------")
      res4DF.show()
      println("------------res4DF end----------")
      val parquetDF1 = spark.sqlContext.read.format("parquet").load(this.getCurProjectRootPath() +
        "src/test/resources/benchmark")
      println(parquetDF1.count())
      val start_time = new Date().getTime
      val df1 = et.train(parquetDF1, "", Map("atRound" -> "2", "relativeError" -> "0.01"
        , "metrics" -> allMetrics
      ))
      df1.show()
      val end_time = new Date().getTime
      println("The elapsed time for normal metrics is : " + (end_time - start_time) / 1000.0 + "s")
    }
  }
}