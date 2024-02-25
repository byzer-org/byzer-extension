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
    
    val infer_params = JSONTool.toJsonStr(params)

    val code =
      s"""from pyjava.api.mlsql import RayContext
         |from byzerllm.apps.byzer_sql import deploy
         |
         |job_config = None
         |if "code_search_path" in context.conf:
         |    job_config = ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],
         |                                        runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}})
         |ray_context = RayContext.connect(globals(), context.conf["rayAddress"],job_config=job_config)
         |infer_params='''${infer_params}'''
         |deploy(infer_params=infer_params,conf=ray_context.conf())
         |""".stripMargin

    val predictCode = """
        |import ray
        |from pyjava.api.mlsql import RayContext
        |from byzerllm.apps.byzer_sql import chat
        |
        |job_config = None
        |if "code_search_path" in context.conf:
        |    job_config = ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],
        |                                        runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}})
        |ray_context = RayContext.connect(globals(), context.conf["rayAddress"],job_config=job_config)
        |chat(ray_context)
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