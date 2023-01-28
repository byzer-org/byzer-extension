package tech.mlsql.plugins.ext.ets.app

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
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

/**
 * 14/11/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class LSCommand(override val uid: String) extends SQLAlg with FileCommonFunctions with MllibFunctions with Functions with BaseParams with ETAuth {

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
      | run command as LSCommand.`` where path="hdfs://localhost:8020/csv" as table1;
      | -- Or you can use the command as follow:
      | !ls "hdfs://localhost:8020/csv";
      | -- Or you can turn on enableMaximumExceedException, and set the maximum to start the strategy of reporting errors
      | -- exceeding the maximum value.
      | run command as LSCommand.`` where path="/opt/spark-3.3.0-bin-hadoop3/sbin" and enableMaximumExceedException=
      | "true" and maximum="27" as table1;
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
    val isEnableMaximumExceedException = params.getOrElse("enableMaximumExceedException", "false").toBoolean
    val isEnabledCount = params.getOrElse("enableCount", "false").toBoolean
    val maximum = params.getOrElse("maximum", "1000").toInt
    if (curPath == null || curPath.isEmpty) {
      return session.emptyDataFrame
    }
    val conf = new Configuration()
    rewriteHadoopConfiguration(conf, params)

    var fsPath = new Path(curPath)
    var fs: FileSystem = null
    if (params.contains("user") && StringUtils.isNotEmpty(params("user"))) {
      if (curPath.startsWith("/")) {
        val tmpPath = conf.get("fs.defaultFS", "file:///")
        if (tmpPath.endsWith("/")) {
          curPath = tmpPath.substring(0, tmpPath.length - 1) + curPath
        } else {
          curPath = tmpPath + curPath
        }
        fsPath = new Path(curPath)
      }
      fs = FileSystem.get(fsPath.toUri, conf, params("user"))
    } else {
      fs = fsPath.getFileSystem(conf)
    }
    val fileStatus = fs.getFileStatus(fsPath)
    if (fileStatus == null) {
      throw new RuntimeException(s"current path can not exists! path:" + curPath)
    }

    val isDir = fileStatus.isDirectory
    var statuses: Array[FileStatus] = null
    if (isDir) {
      statuses = fs.listStatus(fsPath)
      if (isEnableMaximumExceedException && maximum > 0) {
        val length = statuses.length
        if (length > maximum) {
          throw new RuntimeException(s"maximum number of file and directory exceeded! The expected maximum number of file is $maximum" +
            s", but is $length.")
        }
      }
    } else {
      statuses = Array(fileStatus)
    }

    val lastFile: Seq[(String, String, String, String, String, Boolean, String, String)] = statuses
      //      .filterNot(_.getPath.getName.endsWith(".tmp.crc"))
      //    #-rw-rw----+  3 username userPermission     0 2020-12-16 10:32    hdfs://xx/xy/xx/test_day_v2/20201216
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
    // TODO: We need to add a create time
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
