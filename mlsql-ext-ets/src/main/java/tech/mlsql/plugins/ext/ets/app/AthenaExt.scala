package tech.mlsql.plugins.ext.ets.app

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.DataFrame
import streaming.dsl.mmlib.{AlgType, Code, Doc, HtmlDoc, ModelType, SQLCode}
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.ets.{BaseScriptAlgExt, ScriptRunner}

/**
 *
 * @Author; Andie Huang
 * @Date: 2021/11/25 11:15 上午
 *
 */
class AthenaExt extends BaseScriptAlgExt {
  override def code_template: String =
    """
      |include lib.`gitee.com/andiehuang/lib-core` where
      |libMirror="gitee.com"
      |and alias="athenalib";
      |include local.`athenalib.datasource.athena.save_data`;
      |""".stripMargin

  override def modelType: ModelType = AlgType

  override def etName: String = "__datasource_athena_query_operator__"

  override def codeExample: Code = Code(SQLCode, "")

  override def doc: Doc = Doc(HtmlDoc, "")

  final val rayAddress: Param[String] = new Param[String](this, "rayAddress",
    FormParams.toJson(Text(
      name = "rayAddress",
      value = "",
      extra = Extra(
        doc =
          """
            | Ray Address. default: 127.0.0.1:10001
          """,
        label = "The ray address",
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
  setDefault(rayAddress, "127.0.0.1:10001")

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val inputTable = params("__dfname__")
    val outputTable = params("__newdfname__")

    val newParams = params - "__dfname__" - "__LINE__" - "__newdfname__"


    val athenaSchema = MLMapping.findAlg("AthenaSchemaExt").train(df, path, params)
    val schema = athenaSchema.collect()(0).get(0)

    val scriptParams = List(s"set inputTable='''${inputTable}''';", s"set outputTable='''${outputTable}''';") ++ newParams.map { case (k, v) =>
      s"""set ${k}='''${v}''';"""
    } ++ List(s"set schema='''${schema}''';")

    val scriptStr = scriptParams.mkString("\n") + "\n" + code_template
    val newDF: DataFrame = ScriptRunner.rubSubJob(
      scriptStr,
      (_df: DataFrame) => {},
      Option(df.sparkSession),
      true,
      true).get
    newDF
  }

  final val access_id: Param[String] = new Param[String](this, "access_id",
    FormParams.toJson(Text(
      name = "access_id",
      value = "",
      extra = Extra(
        doc =
          """
            | access_id for the athena account
          """,
        label = "The access id for the athena account",
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


  final val access_key: Param[String] = new Param[String](this, "access_key",
    FormParams.toJson(Text(
      name = "access_key",
      value = "",
      extra = Extra(
        doc =
          """
            | access_key for the athena account
          """,
        label = "The access key for the athena account",
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

  final val region: Param[String] = new Param[String](this, "region",
    FormParams.toJson(Text(
      name = "region",
      value = "",
      extra = Extra(
        doc =
          """
            | region for the athena account
          """,
        label = "The region info for the athena account",
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

  final val database: Param[String] = new Param[String](this, "database",
    FormParams.toJson(Text(
      name = "database",
      value = "",
      extra = Extra(
        doc =
          """
            | the database that you wanna query
          """,
        label = "the database that you wanna query",
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

  final val s3_bucket: Param[String] = new Param[String](this, "s3_bucket",
    FormParams.toJson(Text(
      name = "s3_bucket",
      value = "",
      extra = Extra(
        doc =
          """
            | the s3_bucket info
            |
          """,
        label = "the s3_bucket info",
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


  final val s3_key: Param[String] = new Param[String](this, "s3_key",
    FormParams.toJson(Text(
      name = "s3_key",
      value = "",
      extra = Extra(
        doc =
          """
            | the key that stores the result of query
            |
          """,
        label = "The key that stores the result of query",
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

  final val query: Param[String] = new Param[String](this, "query",
    FormParams.toJson(Text(
      name = "query",
      value = "",
      extra = Extra(
        doc =
          """
            | the query statement
            |
          """,
        label = "Query statement",
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

}