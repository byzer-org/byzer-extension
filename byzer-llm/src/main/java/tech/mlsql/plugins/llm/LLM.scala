package tech.mlsql.plugins.llm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

import tech.mlsql.ets.{PythonCommand, Ray}
import tech.mlsql.version.VersionCompatibility


/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 * !llm finetune _ -model_path xxxxx;
 */
class LLM(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val localPathPrefix = params.getOrElse("localPathPrefix", "/tmp")
    val trainer = new Ray()

    val localModelDir = params.getOrElse("localModelDir", "")

    params.getOrElse("action", "finetune") match {
      case "infer" =>

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
          "quantization_bit=4,"
        } else ""

        val code =
          s"""import ray
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
        trainer.predict(df.sparkSession, modelTable, udfName, Map(
          "registerCode" -> code,
          "sourceSchema" -> "st(field(value,string))",
          "outputSchema" -> "st(field(value,array(string)))"
        ) ++ params)
        df.sparkSession.emptyDataFrame
      case "finetune" =>


        val learningRate = params.getOrElse("learningRate", "1e-2")
        val devices = params.getOrElse("devices", "-1")
        val maxSteps = params.getOrElse("maxSteps", "100")
        val saveSteps = params.getOrElse("saveSteps", "50")
        val pretrainedModel = params.getOrElse("pretrainedModel", "byzerllm/chatglm6b")

        val code =
          s"""from pyjava.api.mlsql import RayContext
             |import os
             |import json
             |import uuid
             |from byzerllm.chatglm6b.finetune import finetune_or_infer,restore_model,load_model
             |from byzerllm.chatglm6b.arguments import ModelArguments,DataTrainingArguments
             |from transformers import Seq2SeqTrainingArguments
             |
             |ray_context = RayContext.connect(globals(),None)
             |
             |rd=str(uuid.uuid4())
             |
             |MODEL_DIR=os.path.join("${localPathPrefix}",rd,"pretrained_model")
             |OUTPUT_DIR=os.path.join("${localPathPrefix}",rd,"finetune_model")
             |DATA_DIR=os.path.join("${localPathPrefix}",rd,"finetune_data")
             |DATA_FILE=os.path.join(DATA_DIR,"data.json")
             |
             |if not os.path.exists(DATA_DIR):
             |    os.makedirs(DATA_DIR)
             |
             |if ${devices} != -1:
             |    os.environ["CUDA_VISIBLE_DEVICES"] = "${devices}"
             |
             |if not "${localModelDir}":
             |    restore_model(ray_context.conf(),MODEL_DIR)
             |else:
             |    MODEL_DIR = "${localModelDir}"
             |
             |with open(DATA_FILE,"w") as f:
             |    for line in ray_context.collect():
             |        s = json.dumps(line,ensure_ascii=False)
             |        f.write(s+"\\n")
             |
             |model_args = ModelArguments(
             |    model_name_or_path=MODEL_DIR,
             |    quantization_bit=4,
             |    pre_seq_len=8
             |)
             |# pre_seq_length=8,
             |training_args = Seq2SeqTrainingArguments(
             |    do_train=True,
             |    do_eval=False,
             |    do_predict=False,
             |    overwrite_output_dir=True,
             |    output_dir=OUTPUT_DIR,
             |    per_device_train_batch_size=1,
             |    per_gpu_eval_batch_size=1,
             |    gradient_accumulation_steps=16,
             |    predict_with_generate=True,
             |    max_steps=${maxSteps},
             |    save_steps=${saveSteps},
             |    learning_rate=${learningRate},
             |    logging_steps=10
             |)
             |
             |data_args = DataTrainingArguments(
             |    max_source_length=64,
             |    max_target_length=64,
             |    prompt_column="instruction",
             |    response_column="output",
             |    train_file=DATA_FILE,
             |    validation_file=DATA_FILE,
             |    overwrite_cache=True
             |)
             |
             |finetune_or_infer(model_args,data_args,training_args)
             |model_binary = load_model(OUTPUT_DIR+"/checkpoint-${maxSteps}")
             |
             |ray_context.build_result(model_binary)""".stripMargin
        logInfo(code)
        trainer.train(df, "", Map(
          "code" -> code
        ) ++ params)
    }


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
