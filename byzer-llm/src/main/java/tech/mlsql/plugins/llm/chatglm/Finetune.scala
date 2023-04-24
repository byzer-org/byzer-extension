package tech.mlsql.plugins.llm.chatglm

import org.apache.spark.sql.DataFrame
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.Ray

/**
 * 4/23/23 WilliamZhu(allwefantasy@gmail.com)
 */
class Finetune(params: Map[String, String]) extends Logging {
  def run(): DataFrame = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val localModelDir = params.getOrElse("localModelDir", "")
    val localPathPrefix = params.getOrElse("localPathPrefix", "/tmp")
    val trainer = new Ray()

    val learningRate = params.getOrElse("learningRate", "1e-2")
    val devices = params.getOrElse("devices", "-1")
    val maxSteps = params.getOrElse("maxSteps", "100")
    val saveSteps = params.getOrElse("saveSteps", "50")
    val pretrainedModel = params.getOrElse("pretrainedModel", "byzerllm/chatglm6b")

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
         |from pyjava.api.mlsql import RayContext
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
    trainer.train(session.emptyDataFrame, "", Map(
      "code" -> code
    ) ++ params)
  }
}
