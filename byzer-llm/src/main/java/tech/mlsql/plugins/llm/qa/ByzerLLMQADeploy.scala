package tech.mlsql.plugins.llm.qa

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.Ray
import tech.mlsql.version.VersionCompatibility

/**
 * 5/12/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerLLMQADeploy(params: Map[String, String]) extends Logging {
  def run(): DataFrame = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val localModelDir = params.getOrElse("localModelDir", "")
    val localPathPrefix = params.getOrElse("localPathPrefix", "/tmp")
    
    val trainer = new Ray()

    val modelTable = params.getOrElse("modelTable", params.getOrElse("model", ""))
    require(modelTable.nonEmpty, "modelTable/model is required")
    val udfName = params("udfName")

    val embeddingFunc = params.getOrElse("embeddingFunc","chat")
    val chatFunc = params.getOrElse("chatFunc","chat")

    val byzerUrl = params.getOrElse("url","http://127.0.0.1:9003/model/predict")

    val devices = params.getOrElse("devices", "-1")
    val infer_params = JSONTool.toJsonStr(params)
    
    val code =
      s"""import os
         |if ${devices} != -1:
         |    os.environ["CUDA_VISIBLE_DEVICES"] = "${devices}"
         |try:
         |    import sys
         |    import logging
         |    import transformers
         |    import datasets
         |    logging.basicConfig(
         |    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
         |    datefmt="%m/%d/%Y %H:%M:%S",
         |    handlers=[logging.StreamHandler(sys.stdout)],)
         |    transformers.utils.logging.set_verbosity_info()
         |    datasets.utils.logging.set_verbosity(logging.INFO)
         |    transformers.utils.logging.set_verbosity(logging.INFO)
         |    transformers.utils.logging.enable_default_handler()
         |    transformers.utils.logging.enable_explicit_format()
         |except ImportError:
         |    pass
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
         |
         |from byzerllm import common_init_model
         |from byzerllm.utils.text_generator import qa_predict_func
         |from pyjava.storage import streaming_tar
         |
         |ray_context = RayContext.connect(globals(), context.conf["rayAddress"])
         |
         |rd=str(uuid.uuid4())
         |
         |MODEL_DIR=os.path.join("${localPathPrefix}",rd,"infer_model")
         |owner = context.conf.get("owner",context.conf["OWNER"])
         |
         |if "${localModelDir}":
         |    MODEL_DIR="${localModelDir}"
         |
         |def init_model(model_refs: List[ClientObjectRef], conf: Dict[str, str]) -> Any:
         |    infer_params = json.loads('''${infer_params}''')
         |    common_init_model(model_refs,conf,MODEL_DIR, is_load_from_local="${localModelDir}" != "")
         |
         |    from byzerllm.apps.qa import ByzerLLMQA
         |    from byzerllm.apps.qa import RayByzerLLMQA
         |    from byzerllm.apps.client import ByzerLLMClient
         |    from byzerllm.apps import ClientParams,QueryParams
         |
         |    qa = ByzerLLMQA(MODEL_DIR,ByzerLLMClient(url="${byzerUrl}",params=ClientParams(
         |      owner=owner,
         |      llm_embedding_func="${embeddingFunc}",
         |      llm_chat_func="${chatFunc}"
         |    )),QueryParams(local_path_prefix="${localPathPrefix}"))
         |    return qa
         |
         |
         |UDFBuilder.build(ray_context,init_model,qa_predict_func)
         |""".stripMargin
    logInfo(code)
    trainer.predict(session, modelTable, udfName, Map(
      "registerCode" -> code,
      "sourceSchema" -> "st(field(value,string))",
      "outputSchema" -> "st(field(value,array(string)))"
    ) ++ params)
    session.emptyDataFrame
  }
}