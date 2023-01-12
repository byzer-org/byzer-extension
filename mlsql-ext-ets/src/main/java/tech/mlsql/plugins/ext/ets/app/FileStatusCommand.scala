package tech.mlsql.plugins.ext.ets.app

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
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
import tech.mlsql.tool.HDFSOperatorV2.hadoopConfiguration

/**
 * 14/11/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class FileStatusCommand(override val uid: String) extends SQLAlg with FileCommonFunctions with MllibFunctions with Functions with BaseParams with ETAuth {

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
      | run command as FileStatusCommand.`` where path="hdfs://localhost:8020/csv" as table1;
      | -- Or you can use the command as follow:
      | !fileStatus "hdfs://localhost:8020/csv";
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
    var curPath = params("path")
    val isEnabledCount = params.getOrElse("enableCount", "false").toBoolean
    if (curPath == null || curPath.isEmpty) {
      return session.emptyDataFrame
    }

    rewriteHadoopConfiguration(hadoopConfiguration, params)

    var fsPath = new Path(curPath)
    var fs: FileSystem = null
    if (params.contains("user") && StringUtils.isNotEmpty(params("user"))) {
      if (curPath.startsWith("/")) {
        val tmpPath = hadoopConfiguration.get("fs.defaultFS", "file:///")
        if (tmpPath.endsWith("/")) {
          curPath = tmpPath.substring(0, tmpPath.length - 1) + curPath
        } else {
          curPath = tmpPath + curPath
        }
        fsPath = new Path(curPath)
      }
      fs = FileSystem.get(fsPath.toUri, hadoopConfiguration, params("user"))
    } else {
      fs = fsPath.getFileSystem(hadoopConfiguration)
    }
    val fileStatus = fs.getFileStatus(fsPath)
    if (fileStatus == null) {
      throw new RuntimeException(s"current path can not exists! path:" + curPath)
    }

    val statuses: Array[FileStatus] = Array(fileStatus)
    val lastFile: Seq[(String, String, String, String, String, Boolean, String, String)] = statuses
      .map { status =>
        def timeFormat = "yyyy-MM-dd HH:mm:SS"

        var len = status.getLen
        if (isEnabledCount && status.isDirectory && len <= 0) {
          len = fs.getContentSummary(status.getPath).getLength
        }

        (status.getPath.getName.split(PathFun.pathSeparator).last,
          status.getPath.toString,
          status.getOwner,
          status.getGroup,
          status.getPermission.toString,
          status.isDirectory,
          len + "",
          new DateTime(status.getModificationTime).toString(timeFormat)
        )
      }

    session.createDataFrame(session.sparkContext.parallelize(lastFile, 1)).toDF(
      "name"
      , "path"
      , "owner"
      , "group"
      , "permission"
      , "isDir"
      , "byteLength"
      , "modificationTime"
    )

  }

}
