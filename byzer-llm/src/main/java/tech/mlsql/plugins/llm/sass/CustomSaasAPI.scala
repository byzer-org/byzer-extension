package tech.mlsql.plugins.llm.sass

import org.apache.spark.sql.DataFrame
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.Ray

class CustomSaasAPI(params: Map[String, String]) extends Logging {
  def run(): DataFrame = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val pretrainedModelType = params("pretrainedModelType")
    val Array(_,model) = pretrainedModelType.split("/")
    val infer_params = JSONTool.toJsonStr(params)
    val trainer = new Ray()
    val modelTable = params.getOrElse("modelTable", params.getOrElse("model", ""))
    require(modelTable.nonEmpty, "modelTable/model is required")
    val udfName = params("udfName")
    val code =
      s"""
         |import os
         |
         |import ray
         |import numpy as np
         |from pyjava.api.mlsql import RayContext,PythonContext
         |
         |from pyjava.udf import UDFMaster,UDFWorker,UDFBuilder,UDFBuildInFunc
         |from typing import Any, NoReturn, Callable, Dict, List
         |import time
         |from ray.util.client.common import ClientActorHandle, ClientObjectRef
         |import uuid
         |import json
         |from byzerllm.saas.${model} import CustomSaasAPI
         |from byzerllm.utils.text_generator import simple_predict_func
         |
         |ray_context = RayContext.connect(globals(), context.conf["rayAddress"])
         |
         |def init_model(model_refs: List[ClientObjectRef], conf: Dict[str, str]) -> Any:
         |    from byzerllm import consume_model
         |    consume_model(conf)
         |    infer_params = json.loads('''${infer_params}''')
         |    infer = CustomSaasAPI(infer_params)
         |    return (infer,None)
         |
         |UDFBuilder.build(ray_context,init_model,simple_predict_func)
         |""".stripMargin

    val predictCode = params.getOrElse("predictCode",
      """
        |import ray
        |from pyjava.api.mlsql import RayContext
        |from byzerllm.apps.byzer_sql import chat
        |ray_context = RayContext.connect(globals(), context.conf["rayAddress"])
        |chat(ray_context)
        |""".stripMargin)

    trainer.predict(session, modelTable, udfName, Map(
      "registerCode" -> code,
      "predictCode" -> predictCode,
      "sourceSchema" -> "st(field(value,string))",
      "outputSchema" -> "st(field(value,array(string)))"
    ) ++ params)
    session.emptyDataFrame
  }
}
