package tech.mlsql.plugins.llm.custom

import org.apache.spark.sql.DataFrame
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.Ray

/**
 * 4/23/23 WilliamZhu(allwefantasy@gmail.com)
 */
class Infer(params: Map[String, String]) extends Logging {
  def run(): DataFrame = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val modelTable = params.getOrElse("modelTable", params.getOrElse("model", "command"))
    val udfName = params("udfName")
    val trainer = new Ray()     
    val infer_params = JSONTool.toJsonStr(params)

    val code =
      s"""from pyjava.api.mlsql import RayContext
         |from byzerllm.apps.byzer_sql import prepare_env,deploy
         |env = prepare_env(globals_info=globals(),context=context)
         |infer_params='''${infer_params}'''
         |deploy(infer_params=infer_params,conf=env.ray_context.conf())
         |""".stripMargin
    logInfo(code)

    val predictCode = """
        |import ray
        |from pyjava.api.mlsql import RayContext
        |from byzerllm.apps.byzer_sql import prepare_env, chat
        |env = prepare_env(globals_info=globals(),context=context)
        |chat(env.ray_context)
        |""".stripMargin
    
    trainer.predict(session, modelTable, udfName, Map(
      "registerCode" -> code,
      "predictCode" -> predictCode,
      "sourceSchema" -> "st(field(value,string))",
      "outputSchema" -> "st(field(value,array(string)))"
    ) ++ params)
    session.emptyDataFrame
  }
}