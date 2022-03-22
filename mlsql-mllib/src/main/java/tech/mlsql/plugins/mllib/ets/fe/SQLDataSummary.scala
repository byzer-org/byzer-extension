package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.{ETMethod, PREDICT}

class SQLDataSummary(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val columns = df.columns
    columns.map(col => {
      if (col.contains(".") || col.contains("`")) {
        throw new RuntimeException(s"The column name : ${col} contains special symbols, like . or `, please rename it first!! ")
      }
    })
    val new_columns = df.schema.filter(sc => {
      sc.dataType match {
        case IntegerType => true
        case DoubleType => true
        case FloatType => true
        case LongType => true
        case _ => false
      }
    }).map(sc => {
      sc.name
    }).toArray
    require(!new_columns.isEmpty,"There is not any numeric columns for Summary. Please double check with the dataType!");
    var new_df = df.select(new_columns.head, new_columns.tail: _*)
    val summary = new_df.describe()
    val quantileNum = new_df.stat.approxQuantile(new_columns, Array(0.25, 0.5, 0.75), 0.05)
    val transpose = quantileNum.transpose
    var transformedRows = transpose.map(row => {
      var newRow = Seq("PLACEHOLDER")
      row.foreach(subRow => {
        newRow = newRow :+ subRow.toString
      })
      newRow
    }
    )

    transformedRows = transformedRows.updated(0, transformedRows(0).updated(0, "25%"))
    transformedRows = transformedRows.updated(1, transformedRows(1).updated(0, "50%"))
    transformedRows = transformedRows.updated(2, transformedRows(2).updated(0, "75%"))
    val appendRows = transformedRows.map(Row.fromSeq(_))
    val spark = df.sparkSession
    val unionedTable = spark.createDataFrame(spark.sparkContext.parallelize(appendRows, 1), summary.schema)
    return summary.union(unionedTable)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
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
      |run table2 as DataSummary.`` as summaryTable;
      |;
    """.stripMargin)

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_data_summary_operator__"),
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


}
