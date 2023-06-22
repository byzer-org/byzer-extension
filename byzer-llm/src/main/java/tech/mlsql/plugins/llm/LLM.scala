package tech.mlsql.plugins.llm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.version.VersionCompatibility


/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 * !llm finetune _ -model_path xxxxx;
 */
class LLM(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val pretrainedModelType = params.getOrElse("pretrainedModelType", "moss")

    params.getOrElse("action", "finetune") match {
      case "infer" =>
        pretrainedModelType match {
          case "chatglm" =>
            import tech.mlsql.plugins.llm.chatglm.PInfer
            val infer = new PInfer(params)
            infer.run
          case "saas/chatglm" =>
            import tech.mlsql.plugins.llm.sass.chatglm.ChatGLMAPI
            val infer = new ChatGLMAPI(params)
            infer.run
          case "saas/sparkdesk" =>
            import tech.mlsql.plugins.llm.sass.sparkdesk.SparkDeskAPI
            val infer = new SparkDeskAPI(params)
            infer.run
          case "moss" =>
            import tech.mlsql.plugins.llm.moss.Infer
            val infer = new Infer(params)
            infer.run
          case "falcon" =>
            import tech.mlsql.plugins.llm.falcon.Infer
            val infer = new Infer(params)
            infer.run
          case "llama" =>
            import tech.mlsql.plugins.llm.falcon.Infer
            val infer = new Infer(params)
            infer.run
          case "dolly" =>
            import tech.mlsql.plugins.llm.dolly.Infer
            val infer = new Infer(params)
            infer.run
          case "bark" =>
            import tech.mlsql.plugins.llm.bark.Infer
            val infer = new Infer(params)
            infer.run
          case "whisper" =>
            import tech.mlsql.plugins.llm.whisper.Infer
            val infer = new Infer(params)
            infer.run
          case "m3e" =>
            import tech.mlsql.plugins.llm.m3e.Infer
            val infer = new Infer(params)
            infer.run
          case "qa" =>
            import tech.mlsql.plugins.llm.qa.ByzerLLMQADeploy
            val infer = new ByzerLLMQADeploy(params)
            infer.run()
          case s if s.startsWith("custom/") =>
            import tech.mlsql.plugins.llm.custom.Infer
            val infer = new Infer(params)
            infer.run
          case _ =>
            throw new RuntimeException(s"${pretrainedModelType} is not supported yet")
        }

      case "finetune" =>
        pretrainedModelType match {
          case "chatglm" =>
            import tech.mlsql.plugins.llm.chatglm.PFinetune
            val finetune = new PFinetune(params)
            finetune.run
          case s if s.startsWith("custom/") =>
            import tech.mlsql.plugins.llm.custom.SFT
            val sft = new SFT(params)
            sft.run
          case "moss" =>
            throw new RuntimeException(s"Finetune ${pretrainedModelType} is not supported yet")
          case _ =>
            throw new RuntimeException(s"Finetune ${pretrainedModelType} is not supported yet")
        }


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
