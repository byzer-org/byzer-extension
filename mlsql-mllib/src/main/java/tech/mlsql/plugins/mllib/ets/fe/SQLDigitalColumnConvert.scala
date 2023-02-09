package tech.mlsql.plugins.mllib.ets.fe

import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, stddev, trim, when}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, Input}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
 * 13/02/2023 hellozepp(lisheng.zhanglin@163.com)
 */
class SQLDigitalColumnConvert(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {
  final val sourceTable: Param[String] = new Param[String](this, "sourceTable",
    FormParams.toJson(Input(
      name = "sourceTable",
      value = "",
      extra = Extra(
        doc =
          """
            | Defines the name of the table copied from this table.
            | e.g. sourceTable = "table1"
          """,
        label = "sourceTable",
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

  final val destTable: Param[String] = new Param[String](this, "destTable",
    FormParams.toJson(Input(
      name = "destTable",
      value = "",
      extra = Extra(
        doc =
          """
            | Defines the name of the table copied to this table.
            | e.g. destTable = "table2"
          """,
        label = "destTable",
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

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_digital_column_convert__"),
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
    val spark = df.sparkSession
    if (!params.contains("sourceTable") || StringUtils.isBlank(params("sourceTable"))
      || !params.contains("destTable") || StringUtils.isBlank(params("destTable"))) {
      throw new IllegalArgumentException("The sourceTable and destTable needs to be specified!")
    }
    val isTrim = params.getOrElse("trim", "true").toBoolean
    val sourceTableName = params("sourceTable").trim.toUpperCase
    val destTableName = params("destTable").trim.toUpperCase
    val sourceTable = spark.table(sourceTableName)
    val destTable = spark.table(destTableName)

    if (sourceTableName.equals(destTableName) || sourceTable.isEmpty || destTable.isEmpty) {
      return destTable
    }
    val numericUpperCaseCols = sourceTable.schema.filter(sc => {
      sc.dataType.typeName match {
        case datatype: String => Array("integer", "int", "short", "double", "float", "long").contains(datatype) || datatype.contains("decimal")
        case _ => false
      }
    }).map(sc => {
      (sc.name.toUpperCase, sc.dataType)
    }).toMap

    val castExprs = destTable.schema.map(sc => {
      val c = destTableName + "." + sc.name
      var castExpr = col(c)
      val upperCaseName = sc.name.toUpperCase
      if (numericUpperCaseCols.contains(upperCaseName)
        && numericUpperCaseCols(upperCaseName) != sc.dataType) {
        if (isTrim) {
          castExpr = trim(col(c)).cast(numericUpperCaseCols(upperCaseName))
        } else {
          castExpr = col(c).cast(numericUpperCaseCols(upperCaseName))
        }
      }
      castExpr.alias(sc.name)
    }).toArray
    destTable.select(castExprs: _*)
  }

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |-- Remove spaces and infer schema via lo a d parameter ignoreLeadingWhiteSpace.
      |LOAD csv.`/tmp/upload/emptyStrTest.csv`
      |where header="true" and inferSchema="true" and ignoreLeadingWhiteSpace="true" and ignoreTrailingWhiteSpace="true" as csvTable1;
      |
      |-- Load a dataset that contains spaces.
      |LOAD csv.`/tmp/upload/emptyStrTest.csv` where header="true" and inferSchema="true" as csvTable2;
      |
      |-- Use DigitalColumnConvert to type the two tables, that is, convert the numeric type string column in the destTable that matches the sourceTable name to a numeric type.
      |run command as DigitalColumnConvert.`` where sourceTable='csvTable1' and destTable='csvTable2' and trim="true" as outputTable;
  """.stripMargin)
}