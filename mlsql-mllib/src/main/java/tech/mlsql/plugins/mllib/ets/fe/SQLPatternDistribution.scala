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

  var exclude_empty_val = true
  var internal_ch_limit = 1000

  def find_patterns(src: String): String = {
    val res = src.toSeq.map(c => c match {
      case p if c >= 'a' && c <= 'z' => 'a'
      case p if c >= 'A' && c <= 'Z' => 'A'
      case p if c >= '0' && c <= '9' => '9'
      case p if c >= 0x4E00 && c <= 0x9FA5 => 'A' // If the character is a Chinese, then retuen 'A'
      case _ => c
    }).toSeq.mkString("")
    if (res.length > 1000)
      res.substring(0, internal_ch_limit)
    else
      res
  }

  def is_need_conclude(freq_map: mutable.Map[String, Int], num_ch_upper: Int, num_ch_lower: Int, num_digit: Int): Boolean = {
    if (num_ch_lower == 0 && freq_map.get("a").get != 0)
      return true
    if (num_ch_upper == 0 && freq_map.get("A").get != 0)
      return true
    if (num_digit == 0 && freq_map.get("9").get != 0)
      return true
    false
  }

  def get_conclude_pattern(freq_map: mutable.Map[String, Int]): String = {
    var res = ""
    if (freq_map.get("A").get != 0) {
      res = freq_map.get("A").get match {
        case 1 => "A"
        case _ => s"A(${freq_map.get("A").get})"
      }
    } else if (freq_map.get("a").get != 0) {
      res = freq_map.get("a").get match {
        case 1 => "a"
        case _ => s"a(${freq_map.get("a").get})"
      }
    } else if (freq_map.get("9").get != 0) {
      res = freq_map.get("9").get match {
        case 1 => "9"
        case _ => s"9(${freq_map.get("9").get})"
      }
    }
    return res
  }

  def find_alternativePatterns(src: String): String = {
    var num_ch_upper = 0
    var num_ch_lower = 0
    var num_digit = 0
    var num_others = 0
    var seq_flag = true // This flag is used for checking if current character is sequential from last one
    // the freq map record the frequency of each pattern
    // the value will be updated to zero when the new pattern is found
    var freq_map = mutable.Map("A" -> 0, "a" -> 0, "9" -> 0, "#" -> 0)
    val res = src.toSeq.map(c => {
      var res = ""
      if (c >= 'a' && c <= 'z') {
        num_ch_lower += 1
        num_ch_upper = 0
        num_digit = 0
        num_others = 0
      } else if (c >= 'A' && c <= 'Z' || c >= 0x4E00 && c <= 0x9FA5) {
        num_ch_lower = 0
        num_ch_upper += 1
        num_digit = 0
        num_others = 0
      } else if (c >= '0' && c <= '9') {
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

      //      if (num_others != 0) {
      //        res = get_conclude_pattern(freq_map) + String.valueOf(c)
      //      }

      // if the it is condlusion needed, or there exists special symbols, we need to get the conclude pattern
      if (is_need_conclude(freq_map, num_ch_upper, num_ch_lower, num_digit) || num_others != 0) {
        // if there exist special symbols, we have to append the special symbols with the pattern
        res = num_others match {
          case 0 => get_conclude_pattern(freq_map)
          case _ => get_conclude_pattern(freq_map) + String.valueOf(c)
        }
      }
      freq_map.update("A", num_ch_upper)
      freq_map.update("a", num_ch_lower)
      freq_map.update("9", num_digit)
      freq_map.update("#", num_others)
      res
    }) ++ Seq(get_conclude_pattern(freq_map))
    res.mkString("")
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val limit = params.getOrElse(limitNum.name, "100").toInt
    exclude_empty_val = params.getOrElse(excludeEmptyVal.name, "true").toBoolean

    var pattern_func = udf(find_patterns(_))
    var pattern_func1 = udf(find_alternativePatterns(_))

    val res = df.schema.map(sc => {
      var col_pattern_map = Map[String, String]()
      sc.dataType match {
        case StringType =>
          val sub_df = exclude_empty_val match {
            case true => df.select(sc.name).where(col(sc.name).isNotNull).where(col(sc.name) =!= "")
            case _ => df.select(sc.name)
          }
          require(!sub_df.isEmpty, s"Please make sure the column ${sc.name} contains content except null or empty content!")
          val rows_num = sub_df.count()
          val res = sub_df.withColumn("pattern", pattern_func(col(sc.name))).withColumn("alternativePattern", pattern_func1(col(sc.name)))
          val pattern_group_df = res.groupBy(col("pattern"), col("alternativePattern")).count().orderBy(desc("count")).withColumn("ratio", col("count") / rows_num.toDouble)
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

  setDefault(limitNum, "100")
  setDefault(excludeEmptyVal, "true")
}