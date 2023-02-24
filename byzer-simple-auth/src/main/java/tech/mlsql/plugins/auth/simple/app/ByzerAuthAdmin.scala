package tech.mlsql.plugins.auth.simple.app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.version.VersionCompatibility

/**
 * 24/2/2023 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerAuthAdmin(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    import df.sparkSession.implicits._

    args match {
      case List("admin", "refresh") =>
        ByzerSimpleAuth.reload
      case _ =>
    }

    df.sparkSession.createDataset[String](Seq().toSeq).toDF("content")

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = ByzerSimpleAuthApp.versions

  override def skipPathPrefix: Boolean = false

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      db = Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      table = Option("__auth_admin__"),
      operateType = OperateType.EMPTY,
      sourceType = Option("_mlsql_"),
      tableType = TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None => List(TableAuthResult(true, ""))
    }
  }
}



