package tech.mlsql.plugins.ds.app

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession, functions => F}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.version.VersionCompatibility

import java.net.URI

/**
 *
 * load xtreme1.`s3://x1-community/2/`
 */
class MLSQLXtreme1(override val uid: String) extends MLSQLBaseFileSource with WowParams with VersionCompatibility {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val session = context.execListener.sparkSession
    val owner = config.config.get("owner").getOrElse(context.owner)
    val jsonPath = resourceRealPath(context.execListener, Option(owner), config.path)
    val jsonFileStr = session.read.format("text").option("wholeFile", "true").
      load(jsonPath).collect().head.getString(0) //HDFSOperatorV2.readFile(jsonPath)
    val xtreme1Format = JSONTool.parseJson[Xtreme1Format](jsonFileStr)
    val rows = xtreme1Format.contents.map(item =>
      Row.fromSeq(Seq(item.data.image.split("/").last.split("\\?").head, JSONTool.toJsonStr(item))))

    val p = new URI(jsonPath)
    val withSchema = p.getScheme != null

    val metaDF = session.createDataFrame(session.sparkContext.parallelize(rows),
      StructType(Seq(StructField("name", StringType), StructField("meta", StringType))))

    val paths = xtreme1Format.contents.map { item =>
      val prefix = if (withSchema) {
        s"${p.getScheme}://x1-community"
      } else {
        ""
      }
      prefix + "/" + item.data.image.split("x1-community").last.split("\\?").head
    }

    val imgDf = session.read.format("binaryFile").load(paths: _*).select(
      F.element_at(F.split(F.col("path"), "/"), -1).alias("name"),
      F.col("content").alias("data"))

    val newDf = metaDF.join(imgDf, metaDF("name") === imgDf("name"), "left").select(metaDF("*"), imgDf("data"))
    newDf

  }


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

  override def supportedVersions: Seq[String] = {
    Seq(">1.6.0")
  }

  override def fullFormat: String = "xtreme1"

  override def shortFormat: String = "xtreme1"
}