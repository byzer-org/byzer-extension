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
    val localModelDir = params.getOrElse("localModelDir", "")
    val localPathPrefix = params.getOrElse("localPathPrefix", "/tmp")
    val trainer = new Ray()

    val modelTable = params.getOrElse("modelTable", params.getOrElse("model", ""))
    require(modelTable.nonEmpty, "modelTable/model is required")
    val udfName = params("udfName")
    val devices = params.getOrElse("devices", "-1")

    // custom/m3e
    val pretrainedModelType = params.getOrElse("pretrainedModelType", "moss")
    val realPretrainedModelType = pretrainedModelType.split("/").last

    val infer_params = JSONTool.toJsonStr(params)

    val predict_func = realPretrainedModelType match {
      case "chatglm2" => "chatglm_predict_func"
      case _ =>  "simple_predict_func"
    }

    val code =
      s"""import ray
         |import numpy as np
         |from pyjava.api.mlsql import RayContext,PythonContext
         |
         |from pyjava.udf import UDFMaster,UDFWorker,UDFBuilder,UDFBuildInFunc
         |from typing import Any, NoReturn, Callable, Dict, List
         |from ray.util.client.common import ClientActorHandle, ClientObjectRef
         |import time
         |import uuid
         |import os
         |import json
         |from pyjava.storage import streaming_tar
         |
         |from byzerllm import common_init_model
         |import byzerllm.${realPretrainedModelType} as infer
         |from byzerllm.utils.text_generator import ${predict_func}
         |
         |job_config = None
         |if "code_search_path" in context.conf:
         |    job_config = ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],
         |                                        runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}})
         |ray_context = RayContext.connect(globals(), context.conf["rayAddress"],job_config=job_config)
         |
         |rd=str(uuid.uuid4())
         |
         |MODEL_DIR=os.path.join("${localPathPrefix}",rd,"infer_model")
         |OUTPUT_DIR=os.path.join("${localPathPrefix}",rd,"checkpoint")
         |
         |if "${localModelDir}":
         |    MODEL_DIR="${localModelDir}"
         |if ${devices} != -1:
         |    os.environ["CUDA_VISIBLE_DEVICES"] = "${devices}"
         |
         |
         |def init_model(model_refs: List[ClientObjectRef], conf: Dict[str, str]) -> Any:
         |    infer_params = json.loads('''${infer_params}''')
         |    common_init_model(model_refs,conf,MODEL_DIR, is_load_from_local="${localModelDir}" != "")
         |    model = infer.init_model(MODEL_DIR,infer_params,conf)
         |    return model
         |
         |UDFBuilder.build(ray_context,init_model,${predict_func})
         |""".stripMargin
    logInfo(code)

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