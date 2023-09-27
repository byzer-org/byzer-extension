package tech.mlsql.plugins.llm

import streaming.core.strategy.platform.PlatformManager
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.{PythonCommand, Ray}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * 9/27/23 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLConfig extends Logging{
  def run() = {

    val context = ScriptSQLExec.context()
    val byzerParams = PlatformManager.getOrCreate.config.get().getParamsMap
    val byzerInstanceName = byzerParams.getOrDefault("streaming.name","default")

    logInfo(s"__MLSQL_CONFIG__${byzerInstanceName} ")

    val session = context.execListener.sparkSession
    val trainer = new Ray()
    val params = mutable.HashMap[String,String]()

    byzerParams.asScala.foreach { case (k, v) =>
      params.put(k, v)
    }

    session.conf.getAll.foreach{case (k,v)=>
      params.put(k,v)
    }



    val rayAddress = params.getOrElse("spark.mlsql.ray.address", "127.0.0.1:10001")
    val conf_params = JSONTool.toJsonStr(params.toMap)

    val confTable = session.createDataFrame(Seq(
      ("conf_params", conf_params)
    )).toDF("key", "value")

    confTable.createOrReplaceTempView("conf_params")
    val df = session.emptyDataFrame

    def buildConfExpr(v: String) = {
      val pythonConf = new PythonCommand()
      val command = JSONTool.toJsonStr(List("conf", v))
      pythonConf.train(df, "", Map("parameters" -> command)).collect()
    }

    def setupDefaultConf = {
      buildConfExpr(s"rayAddress=${rayAddress}")
      buildConfExpr("pythonExec=python")
      buildConfExpr("dataMode=model")
      buildConfExpr("runIn=driver")
      buildConfExpr("num_gpus=0")
      buildConfExpr("standalone=false")
      buildConfExpr("maxConcurrency=1")
      buildConfExpr("infer_backend=transformers")
      buildConfExpr("masterMaxConcurrency=1000")
      buildConfExpr("workerMaxConcurrency=1")
      buildConfExpr(s"owner=${context.owner}")
      buildConfExpr("schema=file")
    }

//    val envSession = new SetSession(session, context.owner)
    setupDefaultConf
    val code =
      s"""
         |import os
         |import json                  
         |from pyjava import RayContext
         |from pyjava.utils.config import create_mlsql_config
         |
         |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
         |sys_conf = ray_context.conf()
         |conf_params = json.loads(sys_conf["conf_params"])
         |create_mlsql_config("${byzerInstanceName}",conf_params)
         |ray_context.build_result([])""".stripMargin
    
    trainer.train(session.emptyDataFrame, "", Map(
      "code" -> code,
      "confTable" -> "conf_params"
    ) ++ params).collect()
  }
}
