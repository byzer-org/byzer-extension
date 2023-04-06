package tech.mlsql.plugins.llm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.common.utils.shell.command.ParamsUtil
import tech.mlsql.ets.Ray
import tech.mlsql.version.VersionCompatibility
import scala.collection.JavaConverters._

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 * !llm finetune _ -model_path xxxxx;
 */
class LLM(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    import df.sparkSession.implicits._

    /**
     * !llm finetune _ -table tableName -max_steps 100
     */
    args.head match {
      case "finetune" =>
        require(args(1) == "_", " _ flag should follow finetune command")
        val parser = new ParamsUtil(args.drop(2).mkString(" "))
        val etParams = parser.getParamsMap.asScala.map(f => (f._1, f._2)).toMap
        val tableName = etParams("table")
        val trainer = new Ray()
        trainer.train(df, "", Map(
          "code" ->
            """from pyjava.api.mlsql import RayContext,PythonContext
              |from pyjava.storage import streaming_tar
              |import os
              |import json
              |from byzerllm.chatglm6b.finetune import finetune_or_infer
              |from byzerllm.chatglm6b.arguments import ModelArguments,DataTrainingArguments
              |from transformers import Seq2SeqTrainingArguments
              |
              |ray_context = RayContext.connect(globals(),None)
              |
              |ray_context.conf()
              |
              |OUTPUT_DIR="/my8t/tmp/adgen-chatglm-6b-pt-8-1e-2"
              |DATA_FILE="/tmp/traindata.json"
              |
              |with open(DATA_FILE,"w") as f:
              |    for line in ray_context.collect():
              |        # json dumps with utf-8
              |        s = json.dumps(line,ensure_ascii=False)
              |        f.write(s+"\n")
              |
              |model_args = ModelArguments(
              |    model_name_or_path="/home/winubuntu/projects/glm-model/chatglm-6b",
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
              |    max_steps=100,
              |    save_steps=50,
              |    learning_rate=1e-2,
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
              |finetune_or_infer(model_args,training_args,data_args)
              |
              |# model_binary = streaming_tar.build_rows_from_file(OUTPUT_DIR)
              |
              |ray_context.build_result([{}])
              |""".stripMargin,
          "inputTable" -> tableName
        ))
      case _ =>
        throw new RuntimeException(s"Invalid command: ${args.headOption.mkString(" ")}")
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
