package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/2/21 22:56
 *
 */
class DataTranspose(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {

  def this() = this(BaseParams.randomUID())

  def pivot(df: DataFrame, indexCols: Array[String], inputCol: String, resCol: String): DataFrame = {
    df.groupBy(indexCols.head, indexCols.tail: _*).pivot(col(inputCol)).sum(resCol)
  }

  def unpivot(df: DataFrame, indexCols: Array[String], unPivotCols: Array[String], mergedColName: String, aggCol: String): DataFrame = {
    val formatedIndexCols = indexCols.map(col => s"`$col`")
    val formatedStr = formatedIndexCols.mkString(",")
    val unPivotColLen = unPivotCols.length.toString
    val formatedUnPivotCols = unPivotCols.map(col => s"'$col',`$col`")
    val formatedUnpivotColsStr = formatedUnPivotCols.mkString(",")
    val newcol = formatedIndexCols++Array(s"stack($unPivotColLen,$formatedUnpivotColsStr) as (`$mergedColName`,`$aggCol`)");
    df.selectExpr(newcol.toList:_*)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val _method = params.getOrElse(method.name, "")
    require(!_method.isEmpty, "The param for the method is required!")
    val indexColsStr = params.getOrElse(indexCols.name, "")
    val _indexCols = indexColsStr.split(",")
    _method match {
      case DataTranspose.PIVOT =>
        val _inputCol = params.getOrElse(inputCol.name, "")
        val _aggCol = params.getOrElse(aggCol.name, "")
        val res = pivot(df, _indexCols, _inputCol, _aggCol)
        res
      case DataTranspose.UNPIVOT =>
        val mergedCol = params.getOrElse(DataTranspose.MERGED_COL, "")
        val unPivotCols = params.getOrElse(unpivotCols.name, "")
        val _unPivotCols = unPivotCols.split(",")
        val _aggCol = params.getOrElse(aggCol.name, "")
        val res = unpivot(df, _indexCols, _unPivotCols, mergedCol, _aggCol)
        res
    }

  }

  val method: Param[String] = new Param[String](this, "method", FormParams.toJson(
    Select(
      name = "method",
      values = List(),
      extra = Extra(
        doc = "",
        label = "",
        options = Map(
        )), valueProvider = Option(() => {
        List(
          KV(Some("method"), Some(DataTranspose.PIVOT)),
          KV(Some("method"), Some(DataTranspose.UNPIVOT))
        )
      })
    )
  ))

  final val indexCols: Param[String] = new Param[String](parent = this
    , name = DataTranspose.INDEX_COLS
    , doc = FormParams.toJson(Text(
      name = DataTranspose.INDEX_COLS
      , value = ""
      , extra = Extra(
        doc = "The index columns for transposing data"
        , label = DataTranspose.INDEX_COLS
        , options = Map(
        )
      )
    )
    )
  )

  final val unpivotCols: Param[String] = new Param[String](parent = this
    , name = DataTranspose.UNPIVOT_COLS
    , doc = FormParams.toJson(Text(
      name = DataTranspose.UNPIVOT_COLS
      , value = ""
      , extra = Extra(
        doc = "The columns for merging together as an unpivot one"
        , label = DataTranspose.UNPIVOT_COLS
        , options = Map(
        )
      )
    )
    )
  )

  final val aggCol: Param[String] = new Param[String](parent = this
    , name = DataTranspose.AGG_COL
    , doc = FormParams.toJson(Text(
      name = DataTranspose.AGG_COL
      , value = ""
      , extra = Extra(
        doc = "The columns for aggregation when processing data"
        , label = DataTranspose.AGG_COL
        , options = Map(
        )
      )
    )
    )
  )

  final val inputCol: Param[String] = new Param[String](parent = this
    , name = DataTranspose.INPUT_COL
    , doc = FormParams.toJson(Text(
      name = DataTranspose.INPUT_COL
      , value = ""
      , extra = Extra(
        doc = "The column for transposition as new multiple columns"
        , label = DataTranspose.INPUT_COL
        , options = Map(
        )
      )
    )
    )
  )

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |set jsonStr='''
      |{"subject":"数学","name":"张三","score":88},
      |{"subject":"语文","name":"张三","score":92}
      |{"subject":"英语","name":"张三","score":77}
      |{"subject":"数学","name":"王五","score":65}
      |{"subject":"语文","name":"王五","score":87}
      |{"subject":"英语","name":"王五","score":90}
      |{"subject":"数学","name":"李雷","score":67}
      |{"subject":"语文","name":"李雷","score":33}
      |{"subject":"英语","name":"李雷","score":24}
      |{"subject":"数学","name":"宫九","score":77}
      |{"subject":"语文","name":"宫九","score":87}
      |{"subject":"英语","name":"宫九","score":90}
      |''';
      |load jsonStr.`jsonStr` as data;
      |run data as DataTranspose.`` where method='pivot'
      |and indexCols='name'
      |and inputCol='subject'
      |and aggCol='score'
      |as data1;
      |
      |
      |-- try to unpivot
      |run data1 as DataTranspose.`` where method='unpivot'
      |and indexCols='name'
      |and mergedCol='sbject'
      |and unPivotCols='数学,英语,语文'
      |and aggCol='score'
      |as res;
      |
      |;
    """.stripMargin)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not supported in Data transposition (Pivot/UnPivot) ET")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not supported in Data transposition (Pivot/UnPivot) ET")
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_datatranspose_operator__"),
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

object DataTranspose {
  val METHOD = "method" // 行转列 pivot, 列转行 unpivot
  val INDEX_COLS = "indexCols"
  val INPUT_COL = "inputCol"
  val AGG_COL = "aggCol"

  val UNPIVOT_COLS = "unPivotCols"
  val NEW_COLNAME = "newColName"
  val MERGED_COL = "mergedCol"
  val PIVOT = "pivot"
  val UNPIVOT = "unpivot"
}
