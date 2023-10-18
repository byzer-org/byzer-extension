package tech.mlsql.plugins.llm.tools

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.{PythonCommand, Ray}
import tech.mlsql.session.SetSession
import tech.mlsql.version.VersionCompatibility

/**
 * 5/12/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ModelAdmin(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    val context = ScriptSQLExec.context()
    val session = context.execListener.sparkSession
    import session.implicits._

    def buildConf(key: String, value: String) = {
      val pythonConf = new PythonCommand()
      val command = JSONTool.toJsonStr(List("conf", s"${key}=${value}"))
      pythonConf.train(df, "", Map("parameters" -> command)).collect()
    }

    def buildConfExpr(v: String) = {
      val pythonConf = new PythonCommand()
      val command = JSONTool.toJsonStr(List("conf", v))
      pythonConf.train(df, "", Map("parameters" -> command)).collect()
    }

    val envSession = new SetSession(session, context.owner)

    def setupDefaultConf = {
      buildConfExpr("rayAddress=127.0.0.1:10001")
      buildConfExpr("pythonExec=python")
      buildConfExpr("dataMode=model")
      buildConfExpr("runIn=driver")
      buildConfExpr("num_gpus=1")
      buildConfExpr("standalone=false")
      buildConfExpr("maxConcurrency=1")
      buildConfExpr("infer_backend=transformers")
      buildConfExpr("masterMaxConcurrency=1000")
      buildConfExpr("workerMaxConcurrency=1")
      buildConfExpr(s"owner=${context.owner}")
      buildConfExpr("schema=file")
    }

    // !byzerllm model remove id
    val v = args match {
      case List("setup", "single") =>
        setupDefaultConf
        envSession.fetchPythonRunnerConf.get.toDF()

      case List("setup", "sft") =>
        setupDefaultConf
        buildConfExpr("runIn=executor")
        envSession.fetchPythonRunnerConf.get.toDF()

      case List("setup", "sfft") =>
        setupDefaultConf
        buildConfExpr("runIn=executor")
        envSession.fetchPythonRunnerConf.get.toDF()

      case List("setup", "data") =>
        setupDefaultConf
        buildConfExpr("dataMode=data")
        buildConfExpr("num_gpus=0")
        buildConfExpr("runIn=executor")
        envSession.fetchPythonRunnerConf.get.toDF()

      case List("setup", "retrieval") =>
        setupDefaultConf
        buildConfExpr("rayAddress=worker")
        buildConfExpr("pythonExec=python")
        buildConfExpr("num_gpus=0")
        buildConfExpr("schema=st(field(value,string))")
        envSession.fetchPythonRunnerConf.get.toDF()

      case List("setup", "retrieval/data") =>
        setupDefaultConf
        buildConfExpr("rayAddress=worker")
        buildConfExpr("pythonExec=python")
        buildConfExpr("num_gpus=0")
        buildConfExpr("schema=st(field(value,string))")
        buildConfExpr("dataMode=data")
        buildConfExpr("runIn=executor")
        envSession.fetchPythonRunnerConf.get.toDF()

      case List("setup", value) =>
        buildConfExpr(value)
        envSession.fetchPythonRunnerConf.get.toDF()



      case List("model", "exists", udfName) =>
        buildConfExpr("schema=st(field(value,boolean))")
        val command = new Ray()
        command.train(df, path, Map(
          "code" ->
            s"""
               |from pyjava.api.mlsql import RayContext,PythonContext
               |import ray
               |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
               |try:
               |  ray.get_actor("${udfName}")
               |  exists = True
               |except Exception as e:
               |  exists = False
               |ray_context.build_result([{"value":exists}])
               |""".stripMargin,
          "inputTable" -> "command",
          "outputTable" -> "test"
        ))

      case List("model", "remove", udfName) =>
        val command = new Ray()
        command.train(df, path, Map(
          "code" ->
            s"""
               |from pyjava.api.mlsql import RayContext,PythonContext
               |import ray
               |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
               |a = ray.get_actor("${udfName}")
               |ray.kill(a)
               |ray_context.build_result([])
               |""".stripMargin,
          "inputTable" -> "command",
          "outputTable" -> "test"
        )).collect()
        df.sparkSession.emptyDataFrame
      case _ =>
        df.sparkSession.emptyDataFrame
    }
    v
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