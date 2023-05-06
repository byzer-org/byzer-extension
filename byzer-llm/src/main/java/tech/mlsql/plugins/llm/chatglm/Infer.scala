package tech.mlsql.plugins.llm.chatglm

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
    val quantizationBitNum = params.getOrElse("quantizationBitNum", "4")

    val quantizationBitCode = if (quantizationBit) {
      s"quantization_bit=${quantizationBitNum},"
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
         |import byzerllm.chatglm6b.finetune as finetune
         |from byzerllm.chatglm6b.arguments import ModelArguments,DataTrainingArguments
         |from transformers import Seq2SeqTrainingArguments
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
         |model_args = ModelArguments(
         |    model_name_or_path=MODEL_DIR,
         |    ${quantizationBitCode}
         |    pre_seq_len=8
         |)
         |
         |training_args = Seq2SeqTrainingArguments(
         |    do_train=False,
         |    do_eval=False,
         |    do_predict=True,
         |    overwrite_output_dir=True,
         |    output_dir=OUTPUT_DIR,
         |    per_device_train_batch_size=1,
         |    predict_with_generate=True
         |)
         |
         |data_args = DataTrainingArguments(
         |    max_source_length=64,
         |    max_target_length=64,
         |    prompt_column="instruction",
         |    response_column="output",
         |    overwrite_cache=True,
         |    dataset_name="predict"
         |)
         |
         |def init_model(model_refs: List[ClientObjectRef], conf: Dict[str, str]) -> Any:
         |    if not "${localModelDir}":
         |      if "standalone" in conf and conf["standalone"]=="true":
         |          finetune.restore_model(conf,MODEL_DIR)
         |      else:
         |          streaming_tar.save_rows_as_file((ray.get(ref) for ref in model_refs),MODEL_DIR)
         |    model = finetune.init_model(model_args,data_args,training_args)
         |    return model
         |
         |def predict_func(model,v):
         |    (trainer,tokenizer) = model
         |    data = [json.loads(item) for item in v]
         |    results=finetune.predict(data, data_args,training_args, trainer, tokenizer)
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
