package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, desc, trim, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import tech.mlsql.common.form.{Extra, FormParams, Input}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.collection.mutable

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/7/13 15:39
 *
 */
class SQLPatternDistribution(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {
  def this() = this(BaseParams.randomUID())
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val limit = params.getOrElse(limitNum.name, "100").toInt
    val excludeEmpty = params.getOrElse(excludeEmptyVal.name, "true").toBoolean
    val internalChLimit = params.getOrElse(patternLimit.name, "1000").toInt

    val find_patterns_udf = udf(SQLPatternDistribution.find_patterns(_, internalChLimit))
    val find_alternative_pattern_udf = udf(SQLPatternDistribution.find_alternativePatterns(_, internalChLimit))
    if (df.isEmpty){
      return df.sparkSession.emptyDataFrame
    }
    val strColumns = df.schema.filter(_.dataType == StringType).map(s => col(s.name))
    val fDf = df.select(strColumns: _*)
    val res = fDf.schema.par.map(sc => {
      var col_pattern_map = Map[String, String]()
      val sub_df = if (excludeEmpty) {
        fDf.select(sc.name).where(col(sc.name).isNotNull).where(trim(col(sc.name)) =!= "")
      } else {
        fDf.select(sc.name)
      }
      val isEmpty = sub_df == null || sub_df.limit(1).collect().length == 0
      if (isEmpty) {
        Seq(sc.name, "{}")
      } else {
        val startTime = System.currentTimeMillis()
        val rows_num = sub_df.count()
        val countTime = System.currentTimeMillis()
        logInfo(s"${sc.name}: count time: ${(countTime - startTime) / 1000d}")

        require(rows_num != 0, s"Please make sure the column ${sc.name} contains content except null or empty content!")

        val res = sub_df.
          withColumn(SQLPatternDistribution.patternColName, find_patterns_udf(col(sc.name))).
          withColumn(SQLPatternDistribution.alternativePatternColName, find_alternative_pattern_udf(col(sc.name)))

        val pattern_group_df = res.groupBy(col(SQLPatternDistribution.patternColName),
          col(SQLPatternDistribution.alternativePatternColName)).count().
          orderBy(desc("count")).
          withColumn("ratio", col("count") / rows_num.toDouble)

        val items = pattern_group_df.limit(limit).collect()
        logInfo(s"${sc.name}: pattern match time ${(System.currentTimeMillis() - countTime) / 1000d}")

        val total_count = pattern_group_df.count()

        val jsonItems = items.map { item =>
          val patternColNameV = item.getAs[String](SQLPatternDistribution.patternColName)
          val alternativePatternColNameV = item.getAs[String](SQLPatternDistribution.alternativePatternColName)
          val count = item.getAs[Long]("count")
          val ratio = item.getAs[Double]("ratio")
          Map("count" -> count, "ratio" -> ratio, SQLPatternDistribution.patternColName -> patternColNameV, SQLPatternDistribution.alternativePatternColName -> alternativePatternColNameV)
        }

        val jsonStrItems = JSONTool.toJsonStr(jsonItems)
        col_pattern_map = col_pattern_map ++ Map("colPatternDistribution" -> jsonStrItems, "totalCount" -> String.valueOf(total_count), "limit" -> String.valueOf(limit))
        Seq(sc.name, JSONTool.toJsonStr(col_pattern_map))
      }
    }).map(Row.fromSeq(_)).toList
    val spark = df.sparkSession
    spark.createDataFrame(spark.sparkContext.parallelize(res, 1), StructType(Seq(StructField("columnName", StringType), StructField("patternDistribution", StringType))))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame =
    train(df, path, params)

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_pattern_distribution_operator__"),
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

  final val limitNum: Param[String] = new Param[String](this, "limit",
    FormParams.toJson(Input(
      name = "limit",
      value = "",
      extra = Extra(
        doc =
          """
            | define the amounts of patterns are shown
            | e.g. limit = "100"
          """,
        label = "limit",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "100",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(limitNum, "100")

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

  final val patternLimit: Param[String] = new Param[String](this, "patternLimit",
    FormParams.toJson(Input(
      name = "patternLimit",
      value = "",
      extra = Extra(
        doc =
          """
            | define the limit number for a pattern
            | e.g. patternLimit = 1000
          """,
        label = "patternLimit",
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
  setDefault(patternLimit, "true")

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
      |select name, age, income from table1 as table2;
      |run table2 as PatternDistribution.`` as pd_table;
      |;
    """.stripMargin)

}

object SQLPatternDistribution {
  val capitalLetterKey = "A"
  val lowerCaseLetterKey = "a"
  val symbolLetterKey = "#"
  val numberKey = "9"

  val capitalLetterKey_index = 0
  val lowerCaseLetterKey_index = 1
  val numberKey_index = 2
  val symbolLetterKey_index = 3
  val patternColName = "pattern"

  val alternativePatternColName = "alternativePattern"

  val excludeEmpty = true
  val internalChLimit = 1000

  def isCapitalLetter(ch: Char): Boolean = {
    if (ch >= 'A' && ch <= 'Z') true else false
  }

  def isLowerLetter(ch: Char): Boolean = {
    if (ch >= 'a' && ch <= 'z') true else false
  }

  def isDigitNumber(ch: Char): Boolean = {
    if (ch >= '0' && ch <= '9') true else false
  }

  def isChineseChar(ch: Char): Boolean = {
    if (ch >= 0x4E00 && ch <= 0x9fbb) true else false
  }

  def find_patterns(src: String, _internalChLimit: Int): String = {
    val srcArray = src
    val len = if (srcArray.length <= _internalChLimit) srcArray.length else _internalChLimit
    val str = new mutable.StringBuilder
    for (i <- 0 until len) {
      val c = srcArray(i)
      if (isLowerLetter(c)) {
        str.append('a')
      }
      else if (isCapitalLetter(c) || isChineseChar(c)) {
        // If the character is a Chinese, then return 'A'
        str.append('A')
      }
      else if (isDigitNumber(c)) {
        str.append('9')
      }
      else {
        str.append(c)
      }
    }
    str.result
  }

  def is_need_conclude(freq_map: Array[Int], num_ch_upper: Int, num_ch_lower: Int, num_digit: Int): Boolean = {
    if (num_ch_lower == 0 && freq_map(lowerCaseLetterKey_index) != 0) return true
    if (num_ch_upper == 0 && freq_map(capitalLetterKey_index) != 0) return true
    if (num_digit == 0 && freq_map(numberKey_index) != 0) return true
    false
  }

  def get_conclude_pattern(freq_map: Array[Int]): String = {
    var res = ""
    val capitalLetterFreq = freq_map(capitalLetterKey_index)
    val lowerCaseFreq = freq_map(lowerCaseLetterKey_index)
    val numFreq = freq_map(numberKey_index)

    if (capitalLetterFreq != 0) {
      res = capitalLetterFreq match {
        case 1 => capitalLetterKey
        case _ => s"${capitalLetterKey}(${capitalLetterFreq})"
      }
    } else if (lowerCaseFreq != 0) {
      res = lowerCaseFreq match {
        case 1 => lowerCaseLetterKey
        case _ => s"${lowerCaseLetterKey}(${lowerCaseFreq})"
      }
    } else if (numFreq != 0) {
      res = numFreq match {
        case 1 => numberKey
        case _ => s"${numberKey}(${numFreq})"
      }
    }
    return res
  }
  def find_alternativePatterns(src: String, _internalChLimit: Int): String = {
    var num_ch_upper@num_ch_lower = 0
    var num_digit@num_others = 0
    // the freq map record the frequency of each pattern
    // the value will be updated to zero when the new pattern is found
    val freq_map = Array(0, 0, 0, 0) //mutable.Map(capitalLetterKey -> 0, lowerCaseLetterKey -> 0, numberKey -> 0, symbolLetterKey -> 0)
    val len = if (src.length <= _internalChLimit) src.length else _internalChLimit
    val res = src.substring(0,len).map(c => {
      var res = ""
      if (isLowerLetter(c)) {
        num_ch_lower += 1
        num_ch_upper = 0
        num_digit = 0
        num_others = 0
      } else if (isCapitalLetter(c) || isChineseChar(c)) {
        num_ch_lower = 0
        num_ch_upper += 1
        num_digit = 0
        num_others = 0
      } else if (isDigitNumber(c)) {
        num_ch_lower = 0
        num_ch_upper = 0
        num_digit += 1
        num_others = 0
      } else {
        num_ch_lower = 0
        num_ch_upper = 0
        num_digit = 0
        num_others += 1
      }

      // if the it is condlusion needed, or there exists special symbols, we need to get the conclude pattern
      if (is_need_conclude(freq_map, num_ch_upper, num_ch_lower, num_digit) || num_others != 0) {
        // if there exist special symbols, we have to append the special symbols with the pattern
        res = num_others match {
          case 0 => get_conclude_pattern(freq_map)
          case _ => get_conclude_pattern(freq_map) + String.valueOf(c)
        }
      }

      freq_map(capitalLetterKey_index) = num_ch_upper
      freq_map(lowerCaseLetterKey_index) = num_ch_lower
      freq_map(numberKey_index) = num_digit
      freq_map(symbolLetterKey_index) = num_others
      res
    }) ++ Seq(get_conclude_pattern(freq_map))
    res.mkString("")
  }
}
