package tech.mlsql.plugins.ds

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.ScriptSQLExec


import tech.mlsql.version.VersionCompatibility
import com.github.saurfang.sas.spark._


class MLSQLSAS(override val uid: String) extends MLSQLBaseFileSource with WowParams with VersionCompatibility {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val owner = config.config.get("owner").getOrElse(context.owner)
    reader.options(rewriteConfig(config.config)).format(format).load(resourceRealPath(context.execListener, Option(owner), config.path))
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", resourceRealPath(context.execListener, Option(owner), config.path))
  }

  override def fullFormat: String = "com.github.saurfang.sas.spark"

  override def shortFormat: String = "sas"

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = ???

  override def supportedVersions: Seq[String] = {
    Seq(">=1.6.0")
  }

}
