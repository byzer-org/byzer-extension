package tech.mlsql.plugins.mllib.app


import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.mllib.ets.fe.{DataTranspose, OnehotExt, PSIExt, SQLColumnsAnalysis, SQLDataSummaryV2, SQLDescriptiveMetrics, SQLDigitalColumnConvert, SQLMissingValueProcess, SQLPatternDistribution, SQLUniqueIdentifier}
import tech.mlsql.plugins.mllib.ets.fintech.scorecard.{SQLBinning, SQLScoreCard}
import tech.mlsql.plugins.mllib.ets.{ClassificationEvaluator, ColumnsExt, RegressionEvaluator, SampleDatasetExt, TakeRandomSampleExt}
import tech.mlsql.version.VersionCompatibility

/**
 * 31/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLMllib extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ClassificationEvaluator", classOf[ClassificationEvaluator].getName)
    ETRegister.register("RegressionEvaluator", classOf[RegressionEvaluator].getName)
    //    ETRegister.register("AutoMLExt", classOf[AutoMLExt].getName)
    ETRegister.register("SampleDatasetExt", classOf[SampleDatasetExt].getName)
    ETRegister.register("TakeRandomSampleExt", classOf[TakeRandomSampleExt].getName)
    ETRegister.register("ColumnsExt", classOf[ColumnsExt].getName)
    ETRegister.register("DataSummary", classOf[SQLDataSummaryV2].getName)
//    ETRegister.register("DataSummaryV1", classOf[SQLDataSummary].getName)
//    ETRegister.register("DataSummaryV0", classOf[DataSummaryOld].getName)
    ETRegister.register("DataMissingValueProcess", classOf[SQLMissingValueProcess].getName)
    ETRegister.register("Binning", classOf[SQLBinning].getName)
    ETRegister.register("ScoreCard", classOf[SQLScoreCard].getName)
    ETRegister.register("DataTranspose", classOf[DataTranspose].getName)
    ETRegister.register("Onehot", classOf[OnehotExt].getName)
    ETRegister.register("PSI", classOf[PSIExt].getName)
    ETRegister.register("DescriptiveMetrics", classOf[SQLDescriptiveMetrics].getName)
    ETRegister.register("PatternDistribution", classOf[SQLPatternDistribution].getName)
    ETRegister.register("UniqueIdentifier", classOf[SQLUniqueIdentifier].getName)
    ETRegister.register("DigitalColumnConvert", classOf[SQLDigitalColumnConvert].getName)
    ETRegister.register("ColumnsAnalysis", classOf[SQLColumnsAnalysis].getName)

    // !columns drop fields from tableName;
    CommandCollection.refreshCommandMapping(Map("columns" ->
      """
        |run {3} as ColumnsExt.`` where action="{0}" and fields="{1}"
        |""".stripMargin))

  }


  override def supportedVersions: Seq[String] = {
    MLSQLMllib.versions
  }
}

object MLSQLMllib {
  val versions = Seq(">=2.0.0")
}