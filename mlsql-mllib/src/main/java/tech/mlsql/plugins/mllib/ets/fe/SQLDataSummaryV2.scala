package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import java.util.Date
import scala.util.{Failure, Success, Try}

class SQLDataSummaryV2(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth with Logging {

  def this() = this(BaseParams.randomUID())

  var round_at = 2

  var numericCols: Array[String] = null

  def colWithFilterBlank(sc: StructField): Column = {
    val col_name = sc.name
    sc.dataType match {
      case DoubleType => col(col_name).isNotNull && !col(col_name).isNaN
      case FloatType => col(col_name).isNotNull && !col(col_name).isNaN
      case StringType => col(col_name).isNotNull && col(col_name) =!= ""
      case _ => col(col_name).isNotNull
    }
  }


  def countColsStdDevNumber(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      if (numeric_columns.contains(c)) {
        val expr = stddev(when(colWithFilterBlank(sc), col(c)))
        when(expr.isNull, lit("")).otherwise(expr).alias(c + "_standardDeviation")
      } else {
        lit("").alias(c + "_standardDeviation")
      }
    }).toArray
  }

  def countColsStdErrNumber(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      if (numeric_columns.contains(c)) {
        val expr = stddev(when(colWithFilterBlank(sc), col(c))) / sqrt(sum(when(colWithFilterBlank(sc), 1).otherwise(0)))
        when(expr.isNull, lit("")).otherwise(expr).alias(c + "_standardError")
      } else {
        lit("").alias(c + "_standardError")
      }
    }).toArray
  }

  def isPrimaryKey(schmea: StructType, approx: Boolean): Array[Column] = {
    schmea.map(sc => {
      val c = sc.name
      val exp1 = if (approx) {
        approx_count_distinct(when(colWithFilterBlank(sc), col(sc.name))) / sum(when(colWithFilterBlank(sc), 1).otherwise(0))
      } else {
        countDistinct(when(colWithFilterBlank(sc), col(sc.name))) / sum(when(colWithFilterBlank(sc), 1).otherwise(0))
      }
      when(exp1 === 1, 1).otherwise(0).alias(sc.name + "_primaryKeyCandidate")
    }).toArray
  }

  def countUniqueValueRatio(schema: StructType, approx: Boolean): Array[Column] = {
    schema.map(sc => {
      // TODO:
      val sum_expr = sum(when(colWithFilterBlank(sc), 1).otherwise(0))
      val divide_expr = if (approx) {
        approx_count_distinct(when(colWithFilterBlank(sc), col(sc.name))) / sum_expr
      } else {
        countDistinct(when(colWithFilterBlank(sc), col(sc.name))) / sum_expr
      }
      val ratio_expr = when(sum_expr === 0, 0.0).otherwise(divide_expr)

      ratio_expr.alias(sc.name + "_uniqueValueRatio")
    }).toArray
  }

  def getMaxNum(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      val max_expr = max(when(colWithFilterBlank(sc), col(c)))
      when(max_expr.isNull, "").otherwise(max_expr.cast(StringType)).alias(c + "_max")
    }).toArray
  }

  def getMinNum(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      val min_expr = min(when(colWithFilterBlank(sc), col(c)))
      when(min_expr.isNull, "").otherwise(min_expr.cast(StringType)).alias(c + "_min")
    }).toArray
  }

  def roundAtSingleCol(sc: StructField, column: Column): Column = {
    if (numericCols.contains(sc.name)) {
      return round(column, round_at).cast(StringType)
    }
    column.cast(StringType)
  }

  def processModeValue(modeCandidates: Array[Row], modeFormat: String): Any = {
    val mode = if (modeCandidates.lengthCompare(2) >= 0) {
      modeFormat match {
        case ModeValueFormat.empty => ""
        case ModeValueFormat.all => "[" + modeCandidates.map(_.get(0).toString).mkString(",") + "]"
        case ModeValueFormat.auto => modeCandidates.head.get(0)
      }
    } else {
      modeCandidates.head.get(0)
    }
    mode
  }

  def isArrayString(mode: Any): Boolean = {
    mode.toString.startsWith("[") && mode.toString.endsWith("]")
  }

  def countNonNullValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sum(when(col(sc.name).isNotNull, 1).otherwise(0))
    }).toArray
  }

  def nullValueCount(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case DoubleType | FloatType => (sum(when(col(sc.name).isNull || col(sc.name).isNaN, 1).otherwise(0))) / (sum(lit(1))).alias(sc.name + "_nullValueRatio")
        case _ => (sum(when(col(sc.name).isNull, 1).otherwise(0))) / (sum(lit(1))).alias(sc.name + "_nullValueRatio")
      }
    }).toArray
  }

  def emptyCount(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sum(when(col(sc.name) === "", 1).otherwise(0)) / sum(lit(1.0)).alias(sc.name + "_blankValueRatio")
    }).toArray
  }

  def getMaxLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case StringType => max(length(col(sc.name))).alias(sc.name + "maximumLength")
        case _ => lit("").alias(sc.name + "maximumLength")
      }
    }).toArray
  }

  def getMinLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case StringType => min(length(col(sc.name))).alias(sc.name + "minimumLength")
        case _ => lit("").alias(sc.name + "minimumLength")
      }
    }).toArray
  }


  def getMeanValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      val new_col = if (numericCols.contains(sc.name)) {
        val avgExp = avg(when(colWithFilterBlank(sc), col(sc.name)))
        //        val roundExp = round(avgExp, round_at)
        when(avgExp.isNull, lit("")).otherwise(avgExp).alias(sc.name + "_mean")
      } else {
        lit("").alias(sc.name + "_mean")
      }
      new_col
    }).toArray
  }

  def getTypeLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType.typeName match {
        case "byte" => lit(1L).alias(sc.name)
        case "short" => lit(2L).alias(sc.name)
        case "integer" => lit(4L).alias(sc.name)
        case "long" => lit(8L).alias(sc.name)
        case "float" => lit(4L).alias(sc.name)
        case "double" => lit(8L).alias(sc.name)
        case "string" => max(length(col(sc.name))).alias(sc.name)
        case "date" => lit(8L).alias(sc.name)
        case "timestamp" => lit(8L).alias(sc.name)
        case "boolean" => lit(1L).alias(sc.name)
        case name: String if name.contains("decimal") => first(lit(16L)).alias(sc.name)
        case _ => lit("").alias(sc.name)
      }
    }).toArray
  }

  def roundNumericCols(df: DataFrame, round_at: Integer): DataFrame = {
    df.select(df.schema.map(sc => {
      sc.dataType match {
        case DoubleType => expr(s"cast (${sc.name} as decimal(38,2)) as ${sc.name}")
        case FloatType => expr(s"cast (${sc.name} as decimal(38,2)) as ${sc.name}")
        case _ => col(sc.name)
      }
    }): _*)
  }

  def dataFormat(resRow: Array[Seq[Any]], metricsIdx: Map[Int, String], roundAt: Int): Array[Seq[Any]] = {
    resRow.map(row => {
      row.zipWithIndex.map(el => {
        val e = el._1
        val round_at = metricsIdx.getOrElse(el._2 - 1, "") match {
          case t if t.endsWith("Ratio") => roundAt + 2
          case _ => roundAt
        }
        var newE = e
        try {
          val v = e.toString.toDouble
          newE = BigDecimal(v).setScale(round_at, BigDecimal.RoundingMode.HALF_UP).toDouble
        } catch {
          case e: Exception => logInfo(e.toString)
        }
        newE
      })
    })
  }

  def getPercentileRows(metrics: Array[String], schema: StructType, df: DataFrame, relativeError: Double): (Array[Array[Double]], Array[String]) = {
    var percentilePoints: Array[Double] = Array()
    var percentileCols: Array[String] = Array()
    if (metrics.contains("%25")) {
      percentilePoints = percentilePoints :+ 0.25
      percentileCols = percentileCols :+ "%25"
    }
    if (metrics.contains("median")) {
      percentilePoints = percentilePoints :+ 0.5
      percentileCols = percentileCols :+ "median"
    }
    if (metrics.contains("%75")) {
      percentilePoints = percentilePoints :+ 0.75
      percentileCols = percentileCols :+ "%75"
    }

    val cols = schema.map(sc => {
      var res = lit(0.0).as(sc.name)
      if (numericCols.contains(sc.name)) {
        res = col(sc.name)
      }
      res
    }).toArray
    val quantileRows: Array[Array[Double]] = df.select(cols: _*).na.fill(0.0).stat.approxQuantile(df.columns, percentilePoints, relativeError)
    (quantileRows, percentileCols)
  }

  def processSelectedMetrics(metrics: Array[String]): Array[String] = {
    val normalMetrics = "maximumLength,minimumLength,uniqueValueRatio,nullValueRatio,blankValueRatio,mean,standardDeviation,standardError,max,min,dataLength,primaryKeyCandidate".split(",")
    val computedMetrics = "%25,median,%75".split(",")
    val modeMetric = "mode".split(",")
    var leftMetrics: Array[String] = Array()
    var rightMetrics: Array[String] = Array()
    var appendMetrics: Array[String] = Array()
    metrics.map(m => {
      m match {
        case metric if normalMetrics.contains(metric) => leftMetrics = leftMetrics :+ metric
        case metric if computedMetrics.contains(metric) => rightMetrics = rightMetrics :+ metric
        case metric if modeMetric.contains(metric) => appendMetrics = appendMetrics :+ metric
        case _ => require(false, "The selected metrics contains unkonwn calculation! " + m)
      }
    })
    leftMetrics ++ rightMetrics ++ appendMetrics
  }

  def getModeValue(schema: StructType, df: DataFrame): Array[Any] = {
    val mode = schema.toList.par.map(sc => {
      val dfWithoutNa = df.select(col(sc.name)).na.drop()
      val modeDF = dfWithoutNa.groupBy(col(sc.name)).count().orderBy(F.desc("count")).limit(2)
      val modeList = modeDF.collect()
      if (modeList.length != 0) {
        modeList match {
          case p if p.length < 2 => p(0).get(0)
          case p if p(0).get(1) == p(1).get(1) => ""
          case _ => modeList(0).get(0)
        }
      } else {
        ""
      }
    }).toArray
    mode
  }

  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    round_at = Integer.valueOf(params.getOrElse("roundAt", "2"))

    val metrics = params.getOrElse(DataSummary.metrics, "dataLength,max,min,maximumLength,minimumLength,mean,standardDeviation,standardError,nullValueRatio,blankValueRatio,uniqueValueRatio,primaryKeyCandidate,median,mode").split(",").filter(!_.equalsIgnoreCase(""))
    val relativeError = params.getOrElse("relativeError", "0.01").toDouble
    val approxCountDistinct = params.getOrElse("approxCountDistinct", "false").toBoolean
    val repartitionDF = df
    val columns = repartitionDF.columns

    columns.map(col => {
      if (col.contains(".") || col.contains("`")) {
        throw new RuntimeException(s"The column name : ${col} contains special symbols, like . or `, please rename it first!! ")
      }
    })

    var start_time = new Date().getTime
    numericCols = repartitionDF.schema.filter(sc => {
      sc.dataType.typeName match {
        case datatype: String => Array("integer", "short", "double", "float", "long").contains(datatype) || datatype.contains("decimal")
        case _ => false
      }
    }).map(sc => {
      sc.name
    }).toArray
    val schema = repartitionDF.schema

    val default_metrics = Map(
      "dataLength" -> getTypeLength(schema),
      "max" -> getMaxNum(schema, numericCols),
      "min" -> getMinNum(schema, numericCols),
      "maximumLength" -> getMaxLength(schema),
      "minimumLength" -> getMinLength(schema),
      "mean" -> getMeanValue(schema),
      "standardDeviation" -> countColsStdDevNumber(schema, numericCols),
      "standardError" -> countColsStdErrNumber(schema, numericCols),
      "nullValueRatio" -> nullValueCount(schema),
      "blankValueRatio" -> emptyCount(schema),
      "uniqueValueRatio" -> countUniqueValueRatio(schema, approxCountDistinct),
      "primaryKeyCandidate" -> isPrimaryKey(schema, approxCountDistinct),
    )
    val processedSelectedMetrics = processSelectedMetrics(metrics)
    val newCols = processedSelectedMetrics.map(name => default_metrics.getOrElse(name, null)).filter(_ != null).flatMap(arr => arr).toArray
    val metricsIdx = processedSelectedMetrics.zipWithIndex.map(t => {
      (t._2, t._1)
    }).toMap
    var resDF = repartitionDF.select(newCols: _*)
    logInfo(s"normal metrics plan:\n${resDF.explain(true)}")
    val rows = resDF.collect()
    val rowN = schema.length
    val ordinaryPosRow = df.columns.map(col_name => String.valueOf(df.columns.indexOf(col_name) + 1)).toSeq
    val normalMetricsRow = (ordinaryPosRow ++ rows(0).toSeq).grouped(rowN).map(_.toSeq).toArray.toSeq.transpose
    var end_time = new Date().getTime

    logInfo("The elapsed time for normal metrics is : " + (end_time - start_time))

    // Calculate Percentile
    start_time = new Date().getTime
    val (quantileRows, quantileCols) = getPercentileRows(processedSelectedMetrics, schema, df, relativeError)
    end_time = new Date().getTime
    logInfo("The elapsed time for percentile metrics is: " + (end_time - start_time))

    var datatype_schema: Array[StructField] = null
    var resRows: Array[Seq[Any]] = null
    quantileCols.length match {
      case 0 =>
        resRows = Range(0, schema.length).map(i => {
          Seq(schema(i).name) ++ normalMetricsRow(i)
        }).toArray
      case _ =>
        resRows = Range(0, schema.length).map(i => {
          Seq(schema(i).name) ++ normalMetricsRow(i) ++ quantileRows(i).toSeq
        }).toArray
    }
    datatype_schema = ("ColumnName" +: "ordinaryPosition" +: processedSelectedMetrics).map(t => {
      StructField(t, StringType)
    })

    start_time = new Date().getTime
    // Calculate Mode
    if (processedSelectedMetrics.contains("mode")) {
      val modeRows = getModeValue(schema, df)
      resRows = Range(0, schema.length).map(i => {
        resRows(i) :+ modeRows(i)
      }).toArray
      end_time = new Date().getTime
      logInfo("The elapsed time for mode metric is: " + (end_time - start_time))
    }


    resRows = dataFormat(resRows, metricsIdx, round_at)
    val resAfterTransformed = resRows.map(row => {
      val t = row.map(e => String.valueOf(e))
      t
    })
    val spark = df.sparkSession
    spark.createDataFrame(spark.sparkContext.parallelize(resAfterTransformed.map(Row.fromSeq(_)), 1), StructType(datatype_schema))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame =
    train(df, path, params)

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

object ModeValueFormat {
  val all = "all"
  val empty = "empty"
  val auto = "auto"
}