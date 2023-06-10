package tech.mlsql.plugins.llm.tools

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.Ray
import tech.mlsql.version.VersionCompatibility

/**
 * 5/12/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ModelAdmin(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    import df.sparkSession.implicits._

    // !byzerllm model remove id
    args match {
      case List("model", "remove",udfName) =>
        val command = new Ray()
        command.train(df,path,Map(
          "code"->
            s"""
              |from pyjava.api.mlsql import RayContext,PythonContext
              |import ray
              |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
              |a = ray.get_actor("${udfName}")
              |ray.kill(a)
              |ray_context.build_result([])
              |""".stripMargin,
          "inputTable"->"command",
          "outputTable"->"test"
        )).collect()
      case _ =>
        "No action found"
    }

     df.sparkSession.emptyDataFrame
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