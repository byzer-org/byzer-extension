package tech.mlsql.plugins.mllib.ets.fe

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.liftweb.json.{DefaultFormats, Extraction}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, desc, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, Input}
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

  val capitalLetterKey = "A"
  val lowerCaseLetterKey = "a"
  val symbolLetterKey = "#"
  val numberKey = "9"

  val patternColName = "pattern"

  val alternativePatternColName = "alternativePattern"

  var excludeEmpty = true
  var internalChLimit = 1000

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
    if (ch >= 0x4E00 && ch <= 0x4E00) true else false
  }

  def find_patterns(src: String): String = {
    val res = src.toSeq.map {
      case c if isLowerLetter(c) => 'a'
      case c if isCapitalLetter(c) => 'A'
      case c if isDigitNumber(c) => '9'
      case c if isChineseChar(c) => 'A' // If the character is a Chinese, then retuen 'A'
      case c => c
    }.mkString("")
    if (res.length > 1000) res.substring(0, internalChLimit) else res
  }

  def is_need_conclude(freq_map: mutable.Map[String, Int], num_ch_upper: Int, num_ch_lower: Int, num_digit: Int): Boolean = {
    if (num_ch_lower == 0 && freq_map(lowerCaseLetterKey) != 0) return true
    if (num_ch_upper == 0 && freq_map(capitalLetterKey) != 0) return true
    if (num_digit == 0 && freq_map(numberKey) != 0) return true
    false
  }

  def get_conclude_pattern(freq_map: mutable.Map[String, Int]): String = {
    var res = ""
    val capitalLetterFreq = freq_map(capitalLetterKey)
    val lowerCaseFreq = freq_map(lowerCaseLetterKey)
    val numFreq = freq_map(numberKey)

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

  def find_alternativePatterns(src: String): String = {
    var num_ch_upper@num_ch_lower = 0
    var num_digit@num_others = 0
    var seq_flag = true // This flag is used for checking if current character is sequential from last one
    // the freq map record the frequency of each pattern
    // the value will be updated to zero when the new pattern is found
    var freq_map = mutable.Map(capitalLetterKey -> 0, lowerCaseLetterKey -> 0, numberKey -> 0, symbolLetterKey -> 0)
    val res = src.toSeq.map(c => {
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
      freq_map.update(capitalLetterKey, num_ch_upper)
      freq_map.update(lowerCaseLetterKey, num_ch_lower)
      freq_map.update(numberKey, num_digit)
      freq_map.update(symbolLetterKey, num_others)
      res
    }) ++ Seq(get_conclude_pattern(freq_map))
    res.mkString("")
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val limit = params.getOrElse(limitNum.name, "100").toInt
    excludeEmpty = params.getOrElse(excludeEmptyVal.name, "true").toBoolean
    internalChLimit = params.getOrElse(patternLimit.name, "1000").toInt

    var find_patterns_udf = udf(find_patterns(_))
    var find_alternative_pattern_udf = udf(find_alternativePatterns(_))

    val res = df.schema.map(sc => {
      var col_pattern_map = Map[String, String]()
      sc.dataType match {
        case StringType =>
          val sub_df = if (excludeEmpty) {
            df.select(sc.name).where(col(sc.name).isNotNull).where(col(sc.name) =!= "")
          } else {
            df.select(sc.name)
          }
          require(!sub_df.isEmpty, s"Please make sure the column ${sc.name} contains content except null or empty content!")
          val rows_num = sub_df.count()
          val res = sub_df.withColumn(patternColName, find_patterns_udf(col(sc.name))).withColumn(alternativePatternColName, find_alternative_pattern_udf(col(sc.name)))
          val pattern_group_df = res.groupBy(col(patternColName), col(alternativePatternColName)).count().orderBy(desc("count")).withColumn("ratio", col("count") / rows_num.toDouble)
          val total_count = pattern_group_df.count()
          val res_json_str = pattern_group_df.limit(limit).toJSON.collectAsList.toString
          col_pattern_map = col_pattern_map ++ Map("colPatternDistribution" -> res_json_str, "totalCount" -> String.valueOf(total_count), "limit" -> String.valueOf(limit))
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val json_str = mapper.writeValueAsString(col_pattern_map)
          Seq(sc.name, json_str)
        case _ =>
          null
      }
    }).filter(_ != null).map(Row.fromSeq(_))
    val spark = df.sparkSession
    import spark.implicits._
    spark.createDataFrame(spark.sparkContext.parallelize(res, 1), StructType(Seq(StructField("columnName", StringType), StructField("patternDistribution", StringType))))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = ???

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
