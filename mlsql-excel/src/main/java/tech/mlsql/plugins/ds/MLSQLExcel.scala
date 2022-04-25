package tech.mlsql.plugins.ds

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

class MLSQLExcelApp extends tech.mlsql.app.App with VersionCompatibility with Logging {


  override def supportedVersions: Seq[String] = {
    Seq(">1.6.0")
  }

  override def run(args: Seq[String]): Unit = {
    val clzz = "tech.mlsql.plugins.ds.MLSQLExcel"
    logInfo(s"Load ds: ${clzz}")
    val dataSource = ClassLoaderTool.classForName(clzz).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
  }
}

class MLSQLExcel(override val uid: String)
  extends MLSQLBaseFileSource
    with WowParams with VersionCompatibility {
  def this() = this(BaseParams.randomUID())

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", resourceRealPath(context.execListener, Option(owner), config.path))
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "com.crealytics.spark.excel"

  override def shortFormat: String = "excel"

  final val header: Param[String] = new Param[String](this, "header", "default false")
  final val dataAddress: Param[String] = new Param[String](this, "dataAddress", "Optional, default: \"A1\"; Currently the following address styles are supported:\n\nB3: Start cell of the data. Reading will return all rows below and all columns to the right. Writing will start here and use as many columns and rows as required.\nB3:F35: Cell range of data. Reading will return only rows and columns in the specified range. Writing will start in the first cell (B3 in this example) and use only the specified columns and rows. If there are more rows or columns in the DataFrame to write, they will be truncated. Make sure this is what you want.\n'My Sheet'!B3:F35: Same as above, but with a specific sheet.\nMyTable[#All]: Table of data. Reading will return all rows and columns in this table. Writing will only write within the current range of the table. No growing of the table will be performed. PRs to change this are welcome.")
  final val inferSchema: Param[String] = new Param[String](this, "inferSchema", "Optional, default: false")
  final val maxRowsInMemory: Param[String] = new Param[String](this, "maxRowsInMemory", "Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)")
  final val workbookPassword: Param[String] = new Param[String](this, "workbookPassword", "Optional, default None. Requires unlimited strength JCE for older JVMs")
  final val treatEmptyValuesAsNulls: Param[String] = new Param[String](this, "treatEmptyValuesAsNulls", "Optional, default: true")
  final val timestampFormat: Param[String] = new Param[String](this, "timestampFormat", "Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]")
  final val sheetName: Param[String] = new Param[String](this, "sheetName", "Optional, For save excel")

  override def supportedVersions: Seq[String] = {
    Seq(">1.6.0")
  }
}
