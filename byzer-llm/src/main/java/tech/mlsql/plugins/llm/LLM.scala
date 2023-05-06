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
            import tech.mlsql.plugins.llm.chatglm.Infer
            val infer = new Infer(params)
            infer.run
          case "moss" =>
            import tech.mlsql.plugins.llm.moss.Infer
            val infer = new Infer(params)
            infer.run
          case "dolly" =>
            import tech.mlsql.plugins.llm.dolly.Infer
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
