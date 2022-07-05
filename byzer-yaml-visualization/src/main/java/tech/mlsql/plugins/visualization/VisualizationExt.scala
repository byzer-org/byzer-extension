package tech.mlsql.plugins.visualization

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.ScriptRunner
import tech.mlsql.version.VersionCompatibility

class VisualizationExt(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    args match {
      case List(dataset, code) =>
        val pr = new PlotlyRuntime()
        val pythonCode = pr.translate(code, dataset)
        val hint = new PythonHint()
        val byzerCode = hint.rewrite(pythonCode, Map())
        val newDF: DataFrame = ScriptRunner.rubSubJob(
          byzerCode,
          (_df: DataFrame) => {},
          Option(df.sparkSession),
          true,
          true).get
        newDF
    }
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = {
    Seq(">=1.6.0")
  }
}
