package tech.mlsql.plugins.eval

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.{BaseETAuth, ETAuth}
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.ets.ScriptRunner
import tech.mlsql.version.VersionCompatibility


class ByzerEval(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams with BaseETAuth {
  def this() = this(BaseParams.randomUID())

  // 
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val context = ScriptSQLExec.context()
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray
    val sparkOpt = Option(df.sparkSession)
    command match {
      case Array(variable) =>
        val script = context.execListener.env().get(variable).getOrElse("")
        val jobRes: DataFrame = ScriptRunner.rubSubJob(
          script,
          (_df: DataFrame) => {},
          sparkOpt,
          true,
          true).get
        jobRes
      case _ => throw new RuntimeException("try !eval variableName")
    }

  }

  override def supportedVersions: Seq[String] = {
    ByzerEvalApp.versions
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |
       |For example:
       |
       |```
       |${codeExample.code}
       |```
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """                     
      |set script="select 1 as a as table1;";
      |!eval script;
      |select * from table1 as output;
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def etName: String = "__eval__"
}
