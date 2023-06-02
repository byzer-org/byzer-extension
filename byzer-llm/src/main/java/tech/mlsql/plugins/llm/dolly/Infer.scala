package tech.mlsql.plugins.llm.dolly

import org.apache.spark.sql.DataFrame
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.{PythonCommand, Ray}

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
    val pythonConf = new PythonCommand()

    //        val num_gpus = params.getOrElse("num_gpus", params.getOrElse("numGPUs", "1"))
    //        val maxConcurrency = params.getOrElse("maxConcurrency", "1")
    //        val command = JSONTool.toJsonStr(List("conf", s"num_gpus=${num_gpus}"))
    //        val command2 = JSONTool.toJsonStr(List("conf", s"maxConcurrency=${maxConcurrency}"))
    //        pythonConf.train(df, "", Map("parameters" -> command))
    //        pythonConf.train(df, "", Map("parameters" -> command2))
    val devices = params.getOrElse("devices", "-1")
    val quantizationBit = params.getOrElse("quantizationBit", "false").toBoolean
    val quantizationBitCode = if (quantizationBit) {
      ",load_in_8bit=True"
    } else ""

    val code =
      s"""try:
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
         |import os
         |import json
         |from byzerllm.dolly.dolly_inference import Inference,restore_model
         |from pyjava.storage import streaming_tar
         |
         |ray_context = RayContext.connect(globals(), context.conf["rayAddress"])
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
         |    if not "${localModelDir}":
         |      if "standalone" in conf and conf["standalone"]=="true":
         |          restore_model(conf,MODEL_DIR)
         |      else:
         |          streaming_tar.save_rows_as_file((ray.get(ref) for ref in model_refs),MODEL_DIR)
         |    else:
         |      from byzerllm import consume_model
         |      consume_model(conf)
         |    infer = Inference(MODEL_DIR ${quantizationBitCode})
         |    return infer
         |
         |def predict_func(model,v):
         |    data = [json.loads(item) for item in v]
         |    results=[{"predict":model(item["instruction"]),"labels":""} for item in data]
         |    return {"value":[json.dumps(results,ensure_ascii=False,indent=4)]}
         |
         |UDFBuilder.build(ray_context,init_model,predict_func)
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
