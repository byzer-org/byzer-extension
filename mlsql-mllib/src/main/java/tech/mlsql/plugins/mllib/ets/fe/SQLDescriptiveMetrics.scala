package tech.mlsql.plugins.mllib.ets.fe

import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import tech.mlsql.common.form.{Extra, FormParams, Input}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.collection.mutable
import scala.util.parsing.json.{JSONArray, JSONObject}

/**
 * 04/07/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class SQLDescriptiveMetrics(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  final val excludeEmptyVal: Param[String] = new Param[String](this, "excludeEmptyVal",
    FormParams.toJson(Input(
      name = "excludeEmptyVal",
      value = "",
      extra = Extra(
        doc =
          """
            | define whether exclude empty value
            | e.g. excludeEmptyVal = "true
          """,
        label = "limit",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "true",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(excludeEmptyVal, "true")

  final val metricSize: Param[String] = new Param[String](this, "metricSize",
    FormParams.toJson(Input(
      name = "metricSize",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. SQL to be analyzed
            | e.g. metricSize = "100"
          """,
        label = "metricSize",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "100",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

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
      |run table2 as DescriptiveMetrics.`` as metricsTable;
      |;
    """.stripMargin)
  setDefault(metricSize, "100")

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_descriptive_metrics_operator__"),
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
    val spark = df.sparkSession
    val metricSize = Integer.valueOf(params.getOrElse("metricSize", "100"))
    if (metricSize <= 0) {
      throw new IllegalArgumentException("The limit parameter `metricSize` is not allowed to be less than 1!")
    }
    if (df.isEmpty){
      return df.sparkSession.emptyDataFrame
    }
    import spark.implicits._
    val descriptiveRes = getDescriptiveMetrics(df, metricSize, params)
    spark.createDataset[(String, String)](descriptiveRes).toDF("columnName", "descriptiveMetrics")
  }

  def getDescriptiveMetrics(df: DataFrame, metricSize: Int, params: Map[String, String]): Array[(String, String)] = {
    val excludeEmpty = params.getOrElse(excludeEmptyVal.name, "true").toBoolean
    df.schema.toArray.map(c => {
      val sub_df = if (excludeEmpty) {
        df.select(c.name).where(col(c.name).isNotNull).where(trim(col(c.name)) =!= "")
      } else {
        df.select(c.name)
      }

      import org.apache.spark.sql.functions
      val countRow: Dataset[Row] = sub_df.groupBy(col(c.name)).count().toDF("word", "count")
        .sort(functions.desc("count")).limit(metricSize)
      val countRes = countRow.sort(functions.asc("word")).collect
      (c.name, convertRowToJSON(countRes))
    })
  }

  def convertRowToJSON(rows: Array[Row]): String = {
    val list = mutable.ListBuffer[JSONObject]()
    rows.map(row => {
      var m: Map[String, Any] = row.getValuesMap(row.schema.fieldNames)
      m = m.filter(v => StringUtils.isNotBlank(v._1)).map(v => {
        if (v._2 == null) (v._1, "") else v
      })
      var word: String = ""
      var count: Any = null
      if (m.contains("word")) word = m("word").toString //else skip
      if (m.contains("count")) count = m("count")
      JSONObject(Map(word -> count))
    }).foreach(list.append(_))
    JSONArray(list.toList).toString
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame =
    train(df, path, params)

}
