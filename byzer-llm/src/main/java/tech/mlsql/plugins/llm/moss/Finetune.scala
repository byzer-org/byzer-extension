package tech.mlsql.plugins.llm.moss

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

    session.emptyDataFrame
  }
}
