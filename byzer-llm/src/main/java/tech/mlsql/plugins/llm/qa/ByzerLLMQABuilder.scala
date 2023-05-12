package tech.mlsql.plugins.llm.qa

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.ets.Ray
import tech.mlsql.version.VersionCompatibility

/**
 * 5/12/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerLLMQABuilder(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val inputTable = params("inputTable")
    val outputTable = params("outputTable")
    val command = new Ray()
    // run command as ByzerLLMQA where qaName="qa" and inputTable="";
    command.train(df, path, Map(
      "code" ->
        s"""
           |from pyjava.api.mlsql import RayContext
           |from pyjava.storage import streaming_tar
           |import uuid
           |import ray
           |from byzerllm.apps.qa import RayByzerLLMQA,ByzerLLMClient,ClientParams
           |
           |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
           |
           |db_dir = "/tmp/model/{}".format(str(uuid.uuid4()))
           |qa = RayByzerLLMQA.remote(db_dir,ByzerLLMClient(params=ClientParams(owner=context.conf["owner"])))
           |
           |bb = ray.get(qa.save.remote(ray_context.data_servers()))
           |
           |ray_context.build_result(bb)
           |""".stripMargin,
      "inputTable"->inputTable,
      "outputTable"->outputTable
    ))
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