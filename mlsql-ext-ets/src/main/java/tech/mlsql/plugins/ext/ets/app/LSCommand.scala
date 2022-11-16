package tech.mlsql.plugins.ext.ets.app

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.tool.HDFSOperatorV2

/**
 * 14/11/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class LSCommand(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  final val path: Param[String] = new Param[String](this, "path",
    FormParams.toJson(Text(
      name = "path",
      value = "",
      extra = Extra(
        doc =
          """
            | Setting an hdfs address
          """,
        label = "Setting an hdfs Address",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  def this() = this(BaseParams.randomUID())

  override def codeExample: Code = Code(SQLCode,
    """
      | run command as LSCommand.`` where path="hdfs://localhost:64066/csv" as table1;
      | Or you can use the command as follow:
      | !ls "hdfs://localhost:64066/csv";
    """.stripMargin)

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_hdfs_ls_operator__"),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame =
    train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    val curPath = params("path")
    if (curPath == null || curPath.isEmpty) {
      return session.emptyDataFrame
    }
    val lastFile = HDFSOperatorV2.listFiles(curPath)
      //      .filterNot(_.getPath.getName.endsWith(".tmp.crc"))
      //    #-rw-rw----+  3 username userPermission     0 2020-12-16 10:32    hdfs://xx/xy/xx/test_day_v2/20201216
      .map { status =>
        def timeFormat = "yyyy-MM-dd HH:mm:SS"

        (status.getPath.getName.split(PathFun.pathSeparator).last,
          status.getPath.toString,
          status.getOwner,
          status.getGroup,
          status.getPermission.toString,
          status.isDirectory,
          if (status.isDirectory) "0" else status.getLen + "",
          new DateTime(status.getModificationTime).toString(timeFormat)
        )
      }
    // TODO: We need to add a create time
    session.createDataFrame(session.sparkContext.parallelize(lastFile, 1)).toDF(
      "name"
      , "path"
      , "owner"
      , "group"
      , "permission"
      , "isDirectory"
      , "byteLength"
      , "modificationTime"
    )

  }

}
