package tech.mlsql.plugins.llm.qa

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.Ray
import tech.mlsql.version.VersionCompatibility

/**
 * 5/12/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerLLMQABuilder(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val inputTable = params("inputTable")
    val outputTable = params("outputTable")
    val localPathPrefix = params.getOrElse("localPathPrefix", "/tmp")

    val batchSize = params.getOrElse("batchSize", "100")
    val chunk_size = params.getOrElse("chunkSize", "600")
    val chunk_overlap = params.getOrElse("chunkOverlap", "0")

    val embeddingFunc = params.getOrElse("embeddingFunc", "chat")
    val chatFunc = params.getOrElse("chatFunc", "chat")

    val byzerUrl = params.getOrElse("url","http://127.0.0.1:9003/model/predict")

    val builder_params = JSONTool.toJsonStr(params)
    
    val command = new Ray()
    // run command as ByzerLLMQA where qaName="qa" and inputTable="";
    command.train(df, path, Map(
      "code" ->
        s"""
           |from pyjava.api.mlsql import RayContext
           |from pyjava.storage import streaming_tar
           |import uuid
           |import ray
           |import os
           |import json
           |from byzerllm.apps.qa import RayByzerLLMQA
           |from byzerllm.apps.client import ByzerLLMClient
           |from byzerllm.apps import ClientParams,BuilderParams
           |
           |ray_context = RayContext.connect(globals(),context.conf["rayAddress"])
           |
           |qa = RayByzerLLMQA(
           |     ByzerLLMClient(url="${byzerUrl}",params=ClientParams(
           |         owner=context.conf["owner"],
           |         llm_embedding_func="${embeddingFunc}",
           |         llm_chat_func="${chatFunc}"
           |                   ))
           |     )
           |builder_params = json.loads('''${builder_params}''')
           |bb = qa.save(ray_context.data_servers(),BuilderParams(
           |                                            local_path_prefix="${localPathPrefix}",
           |                                            batch_size=${batchSize},
           |                                            chunk_size=${chunk_size},
           |                                            chunk_overlap=${chunk_overlap}
           |                                            ),builder_params=builder_params)
           |
           |ray_context.build_result(bb)
           |""".stripMargin,
      "inputTable"->inputTable,
      "outputTable"->outputTable
    ))
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