package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ZippedWithGivenIndexRDD
import org.apache.spark.ml.param.Param
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import tech.mlsql.common.form.{Extra, FormParams, Input, KV, Select}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.annotation.tailrec

/**
 * 27/07/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class SQLUniqueIdentifier(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {
  final val SOURCE_MODE_NEW = "new"
  final val SOURCE_MODE_REPLACE = "replace"
  final val DEFAULT_COLUMN_NAME = "Unique_ID"
  final val sourceOptionalVal = List(KV(Option("source"), Option(SOURCE_MODE_NEW)),
    KV(Option("source"), Option(SOURCE_MODE_REPLACE))
  )

  final val source: Param[String] = new Param[String](this, "source",
    FormParams.toJson(Select(
      name = "source",
      values = List(),
      extra = Extra(
        doc =
          s"""
            | unique source
            | When the value is `new`, the behavior is to create a new column, and you need to specify the column name of the new column, the default is `$DEFAULT_COLUMN_NAME`.
            | When the value is `replace`, the behavior is to replace the existing column.
            |  > Note that if a new column is created with a column name that conflicts with an existing column name, an
            |  > error message should be reported
            | e.g. source = "new"
          """,
        label = "action for syntax analysis",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        sourceOptionalVal
      })
    )
    )
  )
  setDefault(source, SOURCE_MODE_NEW)

  final val columnName: Param[String] = new Param[String](this, "columnName",
    FormParams.toJson(Input(
      name = "columnName",
      value = "",
      extra = Extra(
        doc =
          """
            | Column names that need to be replaced or created.
            | e.g. columnName = "age"
          """,
        label = "columnName",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> DEFAULT_COLUMN_NAME,
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(columnName, DEFAULT_COLUMN_NAME)

  def this() = this(BaseParams.randomUID())

  override def codeExample: Code = Code(SQLCode,
    """
      |
      |set abc='''
      |{"name": "elena", "age": 57, "phone": 15552231521, "income": 433000, "label": 0}
      |{"name": "candy", "age": 67, "phone": 15552231521, "income": 1200, "label": 0}
      |{"name": "bob", "age": 57, "phone": 15252211521, "income": 89000, "label": 0}
      |{"name": "candy", "age": 25, "phone": 15552211522, "income": 36000, "label": 1}
      |{"name": "candy", "age": 31, "phone": 15552211521, "income": 300000, "label": 1}
      |{"name": "finn", "age": 23, "phone": 15552211521, "income": 238000, "label": 1}
      |''';
      |
      |load jsonStr.`abc` as table1;
      |select age, income from table1 as table2;
      |run table2 as UniqueIdentifier.`` where source="replace" and columnName="income" as uniqueIdentifier;
      |;
    """.stripMargin)

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_unique_identifier_operator__"),
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

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    var sourceCol = String.valueOf(params.getOrElse(source.name, SOURCE_MODE_NEW)).toLowerCase()
    val columnNameCol = String.valueOf(params.getOrElse(columnName.name, DEFAULT_COLUMN_NAME))
    if (!sourceOptionalVal.contains(KV(Option(source.name), Option(sourceCol)))) {
      throw new IllegalArgumentException(s"Illegal source parameter: $sourceCol")
    }

    val fieldNames = df.schema.map(sc => {
      sc.name
    }).toSet

    val spark = df.sparkSession
    val zipRdd = new ZippedWithGivenIndexRDD(df.rdd, 1)

    @tailrec
    def getRowRDD: DataFrame = sourceCol match {
      case SOURCE_MODE_NEW =>
        if (fieldNames.contains(columnNameCol)) {
          throw new IllegalArgumentException(s"The newly created column name `$columnNameCol` already exists.")
        }

        spark.createDataFrame(zipRdd.map { case (row, index) => Row.fromSeq(index +: row.toSeq) },
          StructType(StructField(columnNameCol, LongType, nullable = false) +: df.schema.fields.toSeq))
      case SOURCE_MODE_REPLACE =>
        /* To avoid exceptions caused by incorrect parameter settings, if the column is not present when replacing the
           schema, a new column is created. */
        if (!fieldNames.contains(columnNameCol)) {
          sourceCol = SOURCE_MODE_NEW
          getRowRDD
        } else {
          var colNumber = -1
          var replaceColNumber = 0
          df.schema.foreach(sc => {
            colNumber += 1
            if (sc.name.equals(columnNameCol)) {
              replaceColNumber = colNumber
              println("ok! replaceColNumber=" + replaceColNumber)
            }
          })
          spark.createDataFrame(zipRdd.map {
            case (row, index) =>
              println(s"replaceColNumber:$replaceColNumber,index:$index")
              Row.fromSeq(row.toSeq.updated(replaceColNumber, index))
          },
            StructType(df.schema.fields.toSeq))
        }
    }

    getRowRDD
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

}
