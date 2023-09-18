package tech.mlsql.plugins.llm.custom

import org.apache.spark.sql.DataFrame
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.Ray

/**
 * 9/11/23 WilliamZhu(allwefantasy@gmail.com)
 */
class LoraMerge (params: Map[String, String]) extends Logging {
  def run(): DataFrame = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val trainer = new Ray()
    val devices = params.getOrElse("devices", "-1")
    val pretrainedModelType = params.getOrElse("pretrainedModelType", "custom/baichuan")
    val realPretrainedModelType = pretrainedModelType.split("/").last
    val train_params = JSONTool.toJsonStr(params)

    val code =
      s"""
         |import os
         |import json
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
         |from pyjava import RayContext
         |try:
         |  from byzerllm.${realPretrainedModelType} import merge_lora_to_base_model
         |except ImportError:
         |  from byzerllm.utils.sft.merge_lora import merge_lora_to_base_model
         |
         |
         |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
         |train_params = json.loads('''${train_params}''')
         |model_binary = merge_lora_to_base_model(ray_context.data_servers(),train_params,ray_context.conf())
         |ray_context.build_result(model_binary)""".stripMargin
    logInfo(code)
    trainer.train(session.emptyDataFrame, "", Map(
      "code" -> code,
      "inputTable" -> "command",
      "outputTable" -> "output",
      "modelTable" -> "command"
    ) ++ params)
  }
}
