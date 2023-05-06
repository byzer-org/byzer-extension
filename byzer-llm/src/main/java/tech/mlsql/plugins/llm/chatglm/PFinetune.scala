package tech.mlsql.plugins.llm.chatglm

import org.apache.spark.sql.DataFrame
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.Ray

/**
 * 4/23/23 WilliamZhu(allwefantasy@gmail.com)
 */
class PFinetune(params: Map[String, String]) extends Logging {
  def run(): DataFrame = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val localModelDir = params.getOrElse("localModelDir", "")
    val localPathPrefix = params.getOrElse("localPathPrefix", "/tmp")
    val trainer = new Ray()

    val learningRate = params.getOrElse("learningRate", "5e-5")
    val finetuningType = params.getOrElse("finetuningType", "lora")
    val numTrainEpochs = params.getOrElse("numTrainEpochs", "1")


    val devices = params.getOrElse("devices", "-1")
    val maxSteps = params.getOrElse("maxSteps", "100")
    val saveSteps = params.getOrElse("saveSteps", "50")


    val pretrainedModel = params.getOrElse("pretrainedModel", "byzerllm/chatglm6b")

    val quantizationBit = params.getOrElse("quantizationBit", "false").toBoolean
    val quantizationBitNum = params.getOrElse("quantizationBitNum", "8")

    val quantizationBitCode = if (quantizationBit) {
      s""" "--quantization_bit", "${quantizationBitNum}" """
    } else ""
    
    val code =
      s"""
         |import os
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
         |from pyjava.api.mlsql import RayContext
         |import json
         |import uuid
         |import shutil
         |import byzerllm.chatglm6b.tunning.finetune as finetune
         |from byzerllm.chatglm6b.finetune import restore_model,load_model
         |
         |ray_context = RayContext.connect(globals(),None)
         |
         |rd=str(uuid.uuid4())
         |
         |MODEL_DIR=os.path.join("${localPathPrefix}",rd,"pretrained_model")
         |OUTPUT_DIR=os.path.join("${localPathPrefix}",rd,"finetune_model")
         |DATA_DIR=os.path.join("${localPathPrefix}",rd,"finetune_data")
         |DATA_FILE=os.path.join(DATA_DIR,"data.jsonl")
         |
         |if not os.path.exists(DATA_DIR):
         |    os.makedirs(DATA_DIR)
         |
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
         |finetune.run([
         | "--do_train",
         | "--model_name_or_path",MODEL_DIR,
         | "--dataset_dir", DATA_DIR,
         | "--finetuning_type", "${finetuningType}",
         | "--per_device_train_batch_size", "4",
         | "--gradient_accumulation_steps", "4",
         | "--lr_scheduler_type", "cosine",
         | "--logging_steps", "10",
         | "--save_steps", "${saveSteps}",
         | "--learning_rate", "${learningRate}",
         | "--max_steps", "${maxSteps}",
         | "--fp16",
         | "--output_dir", OUTPUT_DIR,
         | ${quantizationBitCode}
         |])
         |new_model_dir=os.path.join(OUTPUT_DIR,"checkpoint-${maxSteps}")
         |if "${finetuningType}" == "lora":
         |    new_dir_path = f"{new_model_dir}/pretrained_model"
         |    shutil.copytree(MODEL_DIR, new_dir_path)
         |model_binary = load_model(new_model_dir)
         |ray_context.build_result(model_binary)""".stripMargin
    logInfo(code)
    trainer.train(session.emptyDataFrame, "", Map(
      "code" -> code
    ) ++ params)
  }
}
