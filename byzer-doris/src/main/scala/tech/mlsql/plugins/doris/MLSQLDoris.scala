package tech.mlsql.plugins.doris

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.plugins.doris.MLSQLDoris.{DORIS_FENODES, DORIS_PASSWORD, DORIS_USER, TABLE_IDENTIFIER}

class MLSQLDoris(override val uid: String) extends MLSQLSource
  with MLSQLSink
  with MLSQLSourceInfo
  with MLSQLSourceConfig
  with MLSQLRegistry with DslTool with WowParams with Logging with WowLog {

  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    parseRef(fullFormat, config.path, dbSplitter, configs => {
      reader.options(configs)
    })

    reader.format(fullFormat)
      .options(config.config)
      .option(TABLE_IDENTIFIER, config.path)
      .load()
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    // Load previous defined config
    parseRef(fullFormat, config.path, dbSplitter, configs => {
      writer.options(configs)
    })

    writer.format(fullFormat)
      .options(config.config)
      .option(TABLE_IDENTIFIER, config.path)
      .mode(config.mode)
      .save()
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(db, table) = parseRef(fullFormat, config.path, dbSplitter, (_: Map[String, String]) => { } )
    SourceInfo(shortFormat, db, table)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "doris"
  override def shortFormat: String = fullFormat

  final val dorisFenodes : Param[String] = new Param[String](parent = this
    , name = DORIS_FENODES
    , doc = FormParams.toJson(Text(
      name = DORIS_FENODES
      , value = ""
      , extra = Extra(
        doc = "Doris FE Nodes, example: localhost:8030"
        , label = DORIS_FENODES
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )

  final val dorisUser: Param[String] = new Param[String](parent = this
    , name = DORIS_USER
    , doc = FormParams.toJson(Text(
      name = DORIS_USER
      , value = ""
      , extra = Extra(
        doc = "Doris user name"
        , label = DORIS_USER
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )

  final val dorisPassword: Param[String] = new Param[String](parent = this
    , name = DORIS_PASSWORD
    , doc = FormParams.toJson(Text(
      name = DORIS_PASSWORD
      , value = ""
      , extra = Extra(
        doc = "Doris password"
        , label = DORIS_PASSWORD
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )

}



object MLSQLDoris {
  val TABLE_IDENTIFIER = "doris.table.identifier"
  val DORIS_FENODES = "doris.fenodes"
  val DORIS_USER = "user"
  val DORIS_PASSWORD = "password"
}
