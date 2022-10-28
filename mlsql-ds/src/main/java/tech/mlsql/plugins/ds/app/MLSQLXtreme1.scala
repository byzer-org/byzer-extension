package tech.mlsql.plugins.ds.app

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.tool.HDFSOperatorV2
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
    val owner = config.config.get("owner").getOrElse(context.owner)
    val jsonPath = resourceRealPath(context.execListener, Option(owner), config.path)
    val jsonFileStr = HDFSOperatorV2.readFile(jsonPath)
    val xtreme1Format = JSONTool.parseJson[Xtreme1Format](jsonFileStr)
    val session = context.execListener.sparkSession
    import session.implicits._
    val rows = xtreme1Format.contents.map(item =>
      Seq(item.data.image.split("/").last, JSONTool.toJsonStr(item)))

    val p = new URI(jsonPath)
    val withSchema = p.getScheme != null

    val metaDf = session.createDataset(rows).toDF("name", "meta")
    val paths = xtreme1Format.contents.map { item =>
      val prefix = if (withSchema) {
        s"${p.getScheme}://x1-community"
      } else {
        ""
      }
      prefix + "/" + item.data.image.split("x1-community").last
    }
    val imgDf = session.read.format("image").load(paths: _*)
    imgDf.createTempView("imgDf")
    metaDf.createTempView("metaDf")

    session.sql(
      """
        |select imgDf.image.data as data, metaDf.meta as meta from imgDf left join metaDf on element_at(split(imgDf.name,"/"),-1) == metaDf.name
        |""".stripMargin)

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