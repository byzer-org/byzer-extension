package tech.mlsql.plugins.mllib.ets.fe

import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SQLDataSummaryV2(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth with Logging {

  def this() = this(BaseParams.randomUID())

  var numericCols: Array[String] = Array()

  def colWithFilterBlank(sc: StructField): Column = {
    val col_name = sc.name
    sc.dataType match {
      case DoubleType => col(col_name).isNotNull && !col(col_name).isNaN
      case FloatType => col(col_name).isNotNull && !col(col_name).isNaN
      case StringType => col(col_name).isNotNull && trim(col(col_name)) =!= ""
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

  def countUniqueValueRatio(schema: StructType, approx: Boolean, numeric_columns: Array[String]): Array[Column] = Array.concat(
    schema.map(sc => {
      val sum_expr = sum(when(colWithFilterBlank(sc), 1).otherwise(0))
      // TODO:  张琳 唯一值比例，count(distinct FIELD_NAME) / count(*)。保留2为小数
      val divide_expr = if (approx) {
        nanvl(approx_count_distinct(when(colWithFilterBlank(sc), col(sc.name))) / sum_expr, lit(0.0))
      } else {
        nanvl(countDistinct(when(colWithFilterBlank(sc), col(sc.name))) / sum_expr, lit(0.0))
      }
      val ratio_expr = when(sum_expr === 0, 0.0).otherwise(divide_expr)
      ratio_expr.alias(sc.name + "_uniqueValueRatio")
    }).toArray,
    schema.map(sc => {
      var count_distinct_expr: Column = lit(0)
        if (!numeric_columns.contains(sc.name)) {
        if (approx) {
          count_distinct_expr = approx_count_distinct(when(colWithFilterBlank(sc), col(sc.name)))
        } else {
          count_distinct_expr = countDistinct(when(colWithFilterBlank(sc), col(sc.name)))
        }
      }
      count_distinct_expr.alias(sc.name + "_count_distinct")
    }).toArray: Array[Column]
  )

  def getMaxNum(schema: StructType): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      val max_expr = max(when(colWithFilterBlank(sc), col(c)))
      when(max_expr.isNull, "").otherwise(max_expr.cast(StringType)).alias(c + "_max")
    }).toArray
  }

  def getMinNum(schema: StructType): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      val min_expr = min(when(colWithFilterBlank(sc), col(c)))
      when(min_expr.isNull, "").otherwise(min_expr.cast(StringType)).alias(c + "_min")
    }).toArray
  }

  def countNonNullValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sum(when(colWithFilterBlank(sc), 1).otherwise(0))
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
      sum(when(trim(col(sc.name)) === "", 1).otherwise(0)) / sum(lit(1.0)).alias(sc.name + "_blankValueRatio")
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

  def dataFormat(resRow: Array[Seq[Any]], metricsIdx: Map[Int, String], roundAt: Int): Array[Seq[Any]] = {
    resRow.map(row => {
      row.zipWithIndex.map(el => {
        val metricName = metricsIdx.getOrElse(el._2, "")
        val round_at = metricName match {
          case t if t.endsWith("Ratio") => roundAt + 2
          case _ => roundAt
        }

        var e = ""
        try {
          if (el._1 != null) {
            e = el._1.toString
          }
          val intTypeList = Array("primaryKeyCandidate", "ordinalPosition", "dataLength", "maximumLength",
            "minimumLength", "nonNullCount", "categoryCount", "totalCount")
          // 'Max' and 'Min' are required to preserve decimal places, even though the value may be a string, because
          // the decimal type appears as a long-tailed decimal
          val nonFormat = Array("columnName", "dataType")
          if (intTypeList.contains(metricName)) {
            e = BigDecimal(e).setScale(0, BigDecimal.RoundingMode.HALF_UP).toLong.toString
          } else if (nonFormat.contains(metricName)) {
            // pass
          } else {
            e = BigDecimal(e).setScale(round_at, BigDecimal.RoundingMode.HALF_UP).toDouble.toString
          }
        } catch {
          case _: Exception => //pass
        }
        e
      })
    })
  }

  def getPercentileRows(metrics: Array[String], schema: StructType, relativeError: Double,
                        numeric_columns: Array[String]): Array[Column] = {
    var percentilePoints: Array[Double] = Array()
    if (metrics.contains("%25")) {
      percentilePoints = percentilePoints :+ 0.25
    }
    if (metrics.contains("median")) {
      percentilePoints = percentilePoints :+ 0.5
    }
    if (metrics.contains("%75")) {
      percentilePoints = percentilePoints :+ 0.75
    }

    var accuracy: Int = 0
    if (relativeError > 0) {
      accuracy = (1 / relativeError).toInt
    }

    val resCols = ArrayBuffer[Column]()
    percentilePoints.zipWithIndex.foreach(p => {
      resCols ++= schema.map(sc => {
        val c = sc.name
        if (numeric_columns.contains(c)) {
          val filterExpr = when(colWithFilterBlank(sc), col(c)).otherwise(lit(""))
          var percentile_expr = percentile_approx(filterExpr, lit(percentilePoints), lit(accuracy)).getItem(p._2)
            .alias(c + "_percentile_approx")
          if (accuracy == 0) {
            percentile_expr = expr("percentile(nanvl(" + c + ",\"\"), " + p._1 + ")")
              .alias(c + "_percentile_approx")
          }
          percentile_expr
        } else {
          lit("").alias(c + "_percentile_approx")
        }
      })
    })
    resCols.toArray
  }

  def processSelectedMetrics(metrics: Array[String]): Array[String] = {
    val selectedMetrics = mutable.HashSet[String]()
    selectedMetrics ++= metrics
    ("%25,median,%75,blankValueRatio,dataLength,dataType,max,maximumLength,mean,min,minimumLength," +
      "nonNullCount,nullValueRatio,skewness,totalCount,standardDeviation,standardError,uniqueValueRatio,categoryCount,primaryKeyCandidate,mode"
      ).split(",")
      .filter(selectedMetrics.contains)
  }

  def getModeValue(schema: StructType, df: DataFrame): Array[Any] = {
    val mode = schema.toList.par.map(sc => {
      val dfWithoutNa = df.select(col(sc.name)).na.drop()
      val modeDF = dfWithoutNa.groupBy(col(sc.name).cast(StringType)).count().orderBy(F.desc("count")).limit(2)
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

  def calculateCountMetrics(df: DataFrame, approxCountDistinct: Boolean, threshold: Double = 0.9,
                            numeric_columns: Array[String]): (Seq[Any], Seq[Any], Seq[Any]) = {
    val schema = df.schema
    val columns = schema.map(sc => sc.name).toArray
    val idxToCol = df.columns.zipWithIndex.map(t => (t._2, t._1)).toMap

    var recalCols: Array[StructField] = Array()
    // TODO: 张琳 countUniqueValueRatio是否可以合并到上面的statistics统计
    // calculate uniqueValue Ratio
    var uniqueValueRatioAndCategory = df.select(countUniqueValueRatio(df.schema, approxCountDistinct, numeric_columns
    ): _*).collect()(0).toSeq
    val indices: Int = uniqueValueRatioAndCategory.length / 2
    uniqueValueRatioAndCategory.zipWithIndex.foreach(e => {
      var ratio = 0.0
      try {
        if (e._1 != null) {
          ratio = String.valueOf(e._1).toDouble
        }
      } catch {
        case e: Exception => logDebug("[uniqueValueRatioAndCategory]ratio is not a double number!" + e.getMessage, e)
      }
      val pos = e._2
      if (pos < indices) {
        if (ratio != 1 && (ratio >= threshold)) {
          val colName = idxToCol.getOrElse(pos, null)
          val colType = schema.filter(sc => sc.name == colName).head
          recalCols = recalCols :+ colType
        }
      }
    })

    if (approxCountDistinct && recalCols.length > 0) {
      // TODO:  张琳 应该可以合并到上一个select，写成if（）
      val replacedRatioDF = df.select(countUniqueValueRatio(StructType(recalCols), approx = false, numeric_columns): _*)
      val replaceURrow = replacedRatioDF.collect().take(1)
      if (replaceURrow.length > 0) {
        val recalIndices: Int = replaceURrow.length / 2
        replacedRatioDF.schema.zipWithIndex.foreach(ele => {
          val idx = ele._2
          val categoryIdx = ele._2 + recalIndices
          if (idx < recalIndices) {
            val sc = ele._1
            val colName = sc.name.split("_")(0)
            val originIdx = columns.indexOf(colName)
            val newValue = replaceURrow(0)(idx)
            val newCategoryValue = replaceURrow(0)(categoryIdx)
            uniqueValueRatioAndCategory = uniqueValueRatioAndCategory.updated(originIdx, newValue)
            uniqueValueRatioAndCategory = uniqueValueRatioAndCategory.updated(originIdx, newCategoryValue)
          }
        })
      }
    }

    val uniqueValueRatio = uniqueValueRatioAndCategory.slice(0, indices)
    val category = uniqueValueRatioAndCategory.slice(indices, indices + indices)

    var isPrimaryKeyRow: Array[Int] = Array()
    uniqueValueRatio.zipWithIndex.foreach(e => {
      var ratio = 0.0
      if (e._1 != null) {
        ratio = String.valueOf(e._1).toDouble
      }

      var isPrimaryKeyE = 0
      if (ratio >= 1) {
        isPrimaryKeyE = 1
      }
      isPrimaryKeyRow = isPrimaryKeyRow :+ isPrimaryKeyE
    })

    (uniqueValueRatio, category, isPrimaryKeyRow.toSeq)
  }

  def getDataType(schema: StructType): Array[Column] = {
    schema.map(f => f.dataType.typeName match {
      case "null" => lit("unknown")
      case _ => lit(f.dataType.typeName)
    }).toArray
  }

  def getSkewness(schema: StructType): Array[Column] = {
    schema.map {
      case sc if numericCols.contains(sc.name) =>
        val ske_expr = skewness(when(colWithFilterBlank(sc), col(sc.name)).otherwise(lit("")))
        when(ske_expr.isNull, "").otherwise(ske_expr.cast(StringType)).alias(sc.name + "_skewness")
      case sc => lit("").alias(sc.name + "_skewness")
    }.toArray
  }

  def getTotalCount(schema: StructType, total: Long): Array[Column] = {
    schema.map {
      sc => lit(total).alias(sc.name + "_totalCount")
    }.toArray
  }

  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val round_at = Integer.valueOf(params.getOrElse("roundAt", "2"))
    val selectedMetrics = params.getOrElse(DataSummary.metrics, "dataType,dataLength,max,min,maximumLength,minimumLength," +
      "mean,standardDeviation,standardError,nullValueRatio,blankValueRatio,nonNullCount,uniqueValueRatio," +
      "primaryKeyCandidate,median,mode,totalCount").split(",").filter(!_.equals(""))
    var relativeError = params.getOrElse("relativeError", "0.01").toDouble
    var approxCountDistinct = params.getOrElse("approxCountDistinct", "true").toBoolean

    val approxThreshold = params.getOrElse("approxThreshold", "100000").toLong

    var smallDatasetAccurately = true
    var isSmallData = false
    var total = -1L
    if (approxThreshold == -1) {
      logInfo(format("Small Dataset calculate accurately is disabled"))
      smallDatasetAccurately = false
    }

    if (selectedMetrics.contains("totalCount") || smallDatasetAccurately) {
      total = df.count()
    }

    if (smallDatasetAccurately) {
      logInfo(format(s"The whole dataset is [${total}] and the approxThreshold is ${approxThreshold}"))
      if (total == 0) {
        return df.sparkSession.emptyDataFrame
      }
      if (total <= approxThreshold) {
        isSmallData = true
      }
    }

    if (smallDatasetAccurately && isSmallData) {
      logInfo(format("To calculate all metrics accurately; reset relativeError to 0.0 ,approxCountDistinct to false."))
      relativeError = 0.0
      approxCountDistinct = false
    }

    val df_columns = df.columns
    val schema = df.schema
    val schemaLength = schema.length
    if (schemaLength == 0) {
      return df.sparkSession.emptyDataFrame
    }
    df_columns.foreach(col => {
      if (col.contains(".") || col.contains("`")) {
        throw new RuntimeException(s"The column name : $col contains special symbols, like . or `, please rename it first!! ")
      }
    })

    var start_time = new Date().getTime
    numericCols = df.schema.filter(sc => {
      sc.dataType.typeName match {
        case datatype: String => Array("integer", "short", "double", "float", "long").contains(datatype) || datatype.contains("decimal")
        case _ => false
      }
    }).map(sc => {
      sc.name
    }).toArray

    val default_metrics = Map(
      "dataLength" -> getTypeLength(schema),
      "max" -> getMaxNum(schema),
      "min" -> getMinNum(schema),
      "maximumLength" -> getMaxLength(schema),
      "minimumLength" -> getMinLength(schema),
      "mean" -> getMeanValue(schema),
      "standardDeviation" -> countColsStdDevNumber(schema, numericCols),
      "standardError" -> countColsStdErrNumber(schema, numericCols),
      "nullValueRatio" -> nullValueCount(schema),
      "blankValueRatio" -> emptyCount(schema),
      "nonNullCount" -> countNonNullValue(schema),
      "dataType" -> getDataType(schema),
      "skewness" -> getSkewness(schema),
      "totalCount" -> getTotalCount(schema, total)
    )
    val processedSelectedMetrics = processSelectedMetrics(selectedMetrics)
    var newCols = processedSelectedMetrics.map(name => default_metrics.getOrElse(name, null)).filter(_ != null).flatten
    if (processedSelectedMetrics.contains("75%") || processedSelectedMetrics.contains("25%") || processedSelectedMetrics
      .contains("median")) {
      newCols = getPercentileRows(processedSelectedMetrics, schema, relativeError, numericCols) ++ newCols
    }

    logInfo("The first step spark expr is : select " + newCols.mkString(","))
    val rows = df.select(newCols: _*).collect()
    var statisticMetricsSeq = rows(0).toSeq
    if (processedSelectedMetrics.contains("uniqueValueRatio") || processedSelectedMetrics.contains("primaryKeyCandidate")
      || processedSelectedMetrics.contains("categoryCount")) {
      var threshold = 0.9
      try {
        threshold = params.getOrElse("threshold", "0.9").toDouble
      } catch {
        case _: Exception => logDebug("threshold is not a double number! threshold: " + params.get("threshold")) //pass
      }
      val (uniqueValueRatioRow, categoryCountRow, isPrimaryKeyRow) = calculateCountMetrics(df, approxCountDistinct,
        threshold, numericCols)
      if (processedSelectedMetrics.contains("uniqueValueRatio")) {
        statisticMetricsSeq = statisticMetricsSeq ++ uniqueValueRatioRow
      }

      if (processedSelectedMetrics.contains("categoryCount")) {
        statisticMetricsSeq = statisticMetricsSeq ++ categoryCountRow
      }

      if (processedSelectedMetrics.contains("primaryKeyCandidate")) {
        statisticMetricsSeq = statisticMetricsSeq ++ isPrimaryKeyRow
      }
    }

    val normalMetricsRow = statisticMetricsSeq.grouped(schemaLength).map(_.toSeq).toArray.toSeq.transpose
    var end_time = new Date().getTime

    logInfo("The elapsed time for normal metrics is : " + (end_time - start_time))
    logInfo("The rowN is:" + schemaLength + ", normalMetricsRow size is : " + normalMetricsRow.length)

    var datatype_schema: Array[StructField] = null
    var resRows: Array[Seq[Any]] = Range(0, schemaLength).map(i => {
      Seq(schema(i).name) ++ Seq(String.valueOf(i + 1)) ++ normalMetricsRow(i)
    }).toArray

    // TODO:  张琳 可以动态判断要不要加
    datatype_schema = ("columnName" +: "ordinalPosition" +: processedSelectedMetrics).map(t => {
      StructField(t, StringType)
    })

    start_time = new Date().getTime
    // Calculate Mode
    if (processedSelectedMetrics.contains("mode")) {
      val modeRows = getModeValue(schema, df)
      resRows = Range(0, schemaLength).map(i => {
        resRows(i) :+ modeRows(i)
      }).toArray
      end_time = new Date().getTime
      logInfo("The elapsed time for mode metric is: " + (end_time - start_time))
    }

    val metricsIdx = datatype_schema.zipWithIndex.map(t => {
      (t._2, t._1.name)
    }).toMap
    resRows = dataFormat(resRows, metricsIdx, round_at)
    val spark = df.sparkSession
    spark.createDataFrame(spark.sparkContext.parallelize(resRows.map(Row.fromSeq), 1), StructType(datatype_schema))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame =
    train(df, path, params)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      set abc='''
      {"name": "elena", "age": 57, "phone": 15552231521, "income": 433000, "label": 0}
      {"name": "candy", "age": 67, "phone": 15552231521, "income": 1200, "label": 0}
      {"name": "bob", "age": 57, "phone": 15252211521, "income": 89000, "label": 0}
      {"name": "candy", "age": 25, "phone": 15552211522, "income": 36000, "label": 1}
      {"name": "candy", "age": 31, "phone": 15552211521, "income": 300000, "label": 1}
      {"name": "finn", "age": 23, "phone": 15552211521, "income": 238000, "label": 1}
      ''';

      load jsonStr.`abc` as table1;
      select age, income from table1 as table2;
      run table2 as DataSummary.`` as summaryTable;
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