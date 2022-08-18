package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{avg, coalesce, col, count, countDistinct, expr, first, last, length, lit, map_zip_with, max, min, monotonically_increasing_id, round, row_number, spark_partition_id, sqrt, stddev, sum, udf, when, window}
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, VarcharType}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.{ETMethod, PREDICT}

import scala.util.Try

class SQLDataSummary(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

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
        //        round(stddev(col(c)), round_at).alias(c)
        val expr = round(stddev(when(colWithFilterBlank(sc), col(c))), round_at)
        coalesce(expr, lit("")).alias(c)
      } else {
        max(lit("")).alias(c)
      }
    }).toArray
  }

  def countColsStdErrNumber(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      if (numeric_columns.contains(c)) {
        val expr = round(stddev(when(colWithFilterBlank(sc), col(c))) /
          sqrt(sum(when(colWithFilterBlank(sc), 1).otherwise(0))), round_at)
        coalesce(expr, lit("")).alias(c)
        //        round(stddev(col(c)) / sqrt(total_count), round_at).alias(c)
      } else {
        max(lit("")).alias(c)
      }
    }).toArray
  }

  def isPrimaryKey(schmea: StructType, numeric_columns: Array[String], total_count: Long): Array[Column] = {
    schmea.map(sc => {
      val c = sc.name
      val exp1 = countDistinct(when(colWithFilterBlank(sc), col(sc.name))) / sum(when(colWithFilterBlank(sc), 1).otherwise(0))
      when(exp1 === 1, 1).otherwise(0)
    }).toArray
  }

  def countUniqueValueRatio(schema: StructType): Array[Column] = {
    schema.map(sc => {
      val sum_expr = sum(when(colWithFilterBlank(sc), 1).otherwise(0))

      val divide_expr = countDistinct(when(colWithFilterBlank(sc), col(sc.name))) / sum(when(colWithFilterBlank(sc), 1).otherwise(0))
      val ratio_expr = when(sum_expr === 0, 0.0).otherwise(divide_expr)
      round(ratio_expr, round_at + 2).alias(sc.name)
    }).toArray
  }

  def getMaxNum(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      if (numeric_columns.contains(c)) {
        // if not consider the empty value
        val max_expr = round(max(when(colWithFilterBlank(sc), col(c))), round_at).alias(c)
        coalesce(max_expr.cast(StringType), lit("")).alias(c)
        //        max(col(c)).cast(StringType).alias(c)
      } else {
        val filter_expr = when(colWithFilterBlank(sc), col(sc.name))
        val max_expr = max(filter_expr)
        coalesce(max_expr.cast(StringType), lit("")).alias(c)
      }
    }).toArray
  }

  def getMinNum(schema: StructType, numeric_columns: Array[String]): Array[Column] = {
    schema.map(sc => {
      val c = sc.name
      if (numeric_columns.contains(c)) {
        coalesce(round(min(when(colWithFilterBlank(sc), col(c))), round_at).cast(StringType), lit("")).alias(c)
        //        min(col(c)).cast(StringType).alias(c)
      } else {
        //        min(col(c)).cast(StringType).alias(c)
        val filter_expr = when(colWithFilterBlank(sc), col(sc.name))
        val min_expr = min(filter_expr)
        coalesce(min_expr.cast(StringType), lit("")).alias(c)
      }
    }).toArray
  }

  def roundAtSingleCol(sc: StructField, column: Column): Column = {
    if (numericCols.contains(sc.name)) {
      return round(column, round_at).cast(StringType)
    }
    column.cast(StringType)
  }

  def maxUdf = udf((arr: Seq[Int]) => {
    val filtered = arr.filterNot(_ == "")
    if (filtered.isEmpty) 0
    else filtered.map(_.toInt).max
  })

  def processModeValue(modeCandidates: Array[Row], modeFormat: String): Any = {
    //    val mode = modeCandidates.length match {
    //      case p: Int if p >= 2 => {
    //        modeFormat match {
    //          case ModeValueFormat.empty => ""
    //          case ModeValueFormat.all => "[" + modeCandidates.map(_.get(0).toString).mkString(",") + "]"
    //          case ModeValueFormat.auto => modeCandidates(0).get(0)
    //        }
    //      }
    //      case _ => modeCandidates(0).get(0)
    //    }
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

  def getModeNum(schema: StructType, numeric_columns: Array[String], df: DataFrame, modeFormat: String): Array[Column] = {

    schema.map(sc => {
      val dfWithoutNa = df.repartition(col(sc.name)).select(col(sc.name)).na.drop()
      if (dfWithoutNa.isEmpty) {
        /* If no alias is set, multiple columns with null indicators will result in the following error:
        org.apache.spark.sql.AnalysisException: Reference 'first()' is ambiguous, could be: first(), first(). */
        first(lit("")).alias(sc.name)
      } else {
        val newDF = dfWithoutNa.groupBy(col(sc.name)).count().orderBy(F.desc("count"))
        val largestCount = newDF.collect().head.get(1)
        val modeCandidates = newDF.select(col(sc.name), col("count")).where(col("count") === largestCount).collect()
        val mode = processModeValue(modeCandidates, modeFormat)
        var modeCol = first(lit(mode)).cast(StringType).alias(sc.name)
        // Here to round the mode value
        if (numeric_columns.contains(sc.name) && mode != "" && !isArrayString(mode)) {
          modeCol = roundAtSingleCol(sc, max(lit(mode))).alias(sc.name)
        }
        modeCol
      }
    }).toArray
  }

  def countNonNullValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      count(sc.name).cast(StringType)
    }).toArray
  }

  def countColsNullNumber(schema: StructType, total_count: Long): Array[Column] = {
    //    schema.map(sc => {
    //      // For proportion calculation, we remain 2 more digits. Hence, we need set up round_at + 2
    //      sc.dataType match {
    //        case DoubleType => round(count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)) / total_count, round_at + 2).alias(sc.name)
    //        case FloatType => round(count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)) / total_count, round_at + 2).alias(sc.name)
    //        case _ => round(count(when(col(sc.name).isNull, sc.name)) / total_count, round_at + 2).alias(sc.name)
    //      }
    //    }).toArray
    schema.map(sc => {
      // For proportion calculation, we remain 2 more digits. Hence, we need set up round_at + 2
      //      sc.dataType match {
      //        case DoubleType => round(col(sc.name) / total_count, round_at + 2).alias(sc.name)
      //        case FloatType => round(count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)) / total_count, round_at + 2).alias(sc.name)
      //        case _ => round(count(when(col(sc.name).isNull, sc.name)) / total_count, round_at + 2).alias(sc.name)
      //      }
      round(col(sc.name) / total_count, round_at + 2).alias(sc.name)
    }).toArray
  }

  def countColsEmptyNumber(columns: Array[String], total_count: Long): Array[Column] = {
    //    columns.map(c => {
    //      count(when(col(c) === "", c)).alias(c)
    //    })
    columns.map(c => {
      round(col(c) / total_count, round_at + 2).alias(c)
    }).toArray
  }

  def nullValueCount(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case DoubleType => count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)).alias("left_" + sc.name)
        case FloatType => count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)).alias("left_" + sc.name)
        case _ => count(when(col(sc.name).isNull, sc.name)).alias("left_" + sc.name)
      }
    }).toArray
  }

  def emptyCount(schema: StructType): Array[Column] = {
    schema.map(sc => {
      count(when(col(sc.name) === "", sc.name)).alias("right_" + sc.name)
    }).toArray
  }

  def getMaxLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case StringType => max(length(col(sc.name)))
        case _ => max(lit("")).alias(sc.name)
      }
    }).toArray
  }

  def getMinLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case StringType => min(length(col(sc.name)))
        case _ => min(lit("")).alias(sc.name)
      }
    }).toArray
  }


  def getMeanValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      val new_col = if (numericCols.contains(sc.name)) {
        val avgExp = avg(when(colWithFilterBlank(sc), col(sc.name)))
        val roundExp = round(avgExp, round_at)
        when(roundExp.isNull, lit("")).otherwise(roundExp).alias(sc.name)
      } else {
        last(lit("")).alias(sc.name)
      }
      new_col
    }).toArray
  }

  /**
   * compute percentile from an unsorted Spark RDD
   *
   * @param data : input data set of Double numbers
   * @param tile : percentile to compute (eg. 85 percentile)
   * @return value of input data at the specified percentile
   */
  def computePercentile(data: RDD[Double], tile: Double): Double = {
    // NIST method; data to be sorted in ascending order
    val r = data.sortBy(x => x)
    val c = r.count()
    val res = if (c == 1) r.first()
    else {
      val n = (tile / 100d) * (c + 1d)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) r.first()
      else {
        val index = r.zipWithIndex().map(_.swap)
        val last = c
        if (k >= c) {
          index.lookup(last - 1).head
        } else {
          val topRow = index.lookup(k - 1)
          topRow.head + d * (index.lookup(k).head - topRow.head)
        }
      }
    }
    r.unpersist()
    res
  }

  def getQuantileNum(schema: StructType, df: DataFrame, numeric_columns: Array[String]): Array[Array[Double]] = {
    schema.map(sc => {
      if (numeric_columns.contains(sc.name)) {
        val new_df = df.select(col(sc.name))
        var res = Array(Double.NaN, Double.NaN, Double.NaN)
        val data = new_df.rdd.map(x => {
          val v = String.valueOf(x(0))
          v match {
            case "" => Double.NaN
            case "null" => Double.NaN
            case _ => v.toDouble
          }
        }).filter(!_.isNaN)
        if (data.isEmpty()) {

        } else {
          val q1 = computePercentile(data, 25)
          val q2 = computePercentile(data, 50)
          val q3 = computePercentile(data, 75)
          res = Array(q1, q2, q3)
        }
        res
      } else {
        Array(Double.NaN, Double.NaN, Double.NaN)
      }
    }).toArray
  }

  def getTypeLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType.typeName match {
        case "byte" => first(lit(1L)).alias(sc.name)
        case "short" => first(lit(2L)).alias(sc.name)
        case "integer" => first(lit(4L)).alias(sc.name)
        case "long" => first(lit(8L)).alias(sc.name)
        case "float" => first(lit(4L)).alias(sc.name)
        case "double" => first(lit(8L)).alias(sc.name)
        case "string" => max(length(col(sc.name))).alias(sc.name)
        case "date" => first(lit(8L)).alias(sc.name)
        case "timestamp" => first(lit(8L)).alias(sc.name)
        case "boolean" => first(lit(1L)).alias(sc.name)
        case name: String if name.contains("decimal") => first(lit(16L)).alias(sc.name)
        case _ => first(lit("")).alias(sc.name)
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


  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    round_at = Integer.valueOf(params.getOrElse("roundAt", "2"))

    val approxSwitch = Try(params.getOrElse("approxSwitch", "false").toBoolean).getOrElse(false)
    val modeFormat = Try(params.getOrElse(DataSummary.modeFormat, ModeValueFormat.empty)).getOrElse(ModeValueFormat.empty)
    var metrics = params.getOrElse(DataSummary.metrics, "").split(",").filter(!_.equalsIgnoreCase(""))
    val repartitionDF = df.repartition(df.schema.map(sc => col(sc.name)).toArray: _*).cache()
    val columns = repartitionDF.columns
    try {
      columns.map(col => {
        if (col.contains(".") || col.contains("`")) {
          throw new RuntimeException(s"The column name : ${col} contains special symbols, like . or `, please rename it first!! ")
        }
      })

      numericCols = repartitionDF.schema.filter(sc => {
        sc.dataType.typeName match {
          case datatype: String => Array("integer", "short", "double", "float", "long").contains(datatype) || datatype.contains("decimal")
          case _ => false
        }
      }).map(sc => {
        sc.name
      }).toArray

      val datatype_schema = ("DataType" +: repartitionDF.schema.map(f => f.name)).map(t => {
        StructField(t, StringType)
      })


      // get the quantile number for the numeric columns
      val spark = repartitionDF.sparkSession
      val total_count = repartitionDF.count()

      var new_quantile_rows = approxSwitch match {
        case true => {
          repartitionDF.select(repartitionDF.schema.map(sc => {
            if (numericCols.contains(sc.name)) {
              col(sc.name)
            } else {
              lit(0.0).as(sc.name)
            }
          }
          ): _*).na.fill(0.0).stat.approxQuantile(repartitionDF.columns, Array(0.25, 0.5, 0.75), 0.05).transpose.map(_.map(String.valueOf(_)).toSeq).map(row =>
            "Q" +: row
          )
        }
        case false => {
          getQuantileNum(repartitionDF.schema, repartitionDF, numericCols).transpose.map(_.map(v =>
            String.valueOf(v) match {
              case "NaN" => ""
              case _ => v.formatted(s"%.${round_at}f")
            }
          ).toSeq).map(row =>
            "Q" +: row
          )
        }
      }
      new_quantile_rows = new_quantile_rows.updated(0, new_quantile_rows.head.updated(0, "%25"))
      new_quantile_rows = new_quantile_rows.updated(1, new_quantile_rows(1).updated(0, "median"))
      new_quantile_rows = new_quantile_rows.updated(2, new_quantile_rows(2).updated(0, "%75"))
      val quantile_df_tmp = new_quantile_rows.map(Row.fromSeq(_)).toSeq
      val quantile_df = spark.createDataFrame(spark.sparkContext.parallelize(quantile_df_tmp, 1), StructType(datatype_schema)).na.fill("")
      val new_quantile_df = quantile_df.select(quantile_df.schema.map(sc => {
        if (!numericCols.contains(sc.name) && !sc.name.equals("DataType")) {
          lit("").as(sc.name)
        } else {
          col(sc.name)
        }
      }): _*)

      val mode_df = repartitionDF.select(getModeNum(repartitionDF.schema, numericCols, repartitionDF, modeFormat): _*).select(lit("mode").alias("metric"), col("*"))
      val maxlength_df = repartitionDF.select(getMaxLength(repartitionDF.schema): _*).select(lit("maximumLength").alias("metric"), col("*"))
      val minlength_df = repartitionDF.select(getMinLength(repartitionDF.schema): _*).select(lit("minimumLength").alias("metric"), col("*"))

      val dfWithUniqueRatio = repartitionDF.select(countUniqueValueRatio(repartitionDF.schema): _*)
      val distinct_proportion_df = dfWithUniqueRatio.select(lit("uniqueValueRatio").alias(DataSummary.metricColumnName), col("*"))

      val is_primary_key_df = dfWithUniqueRatio.select(dfWithUniqueRatio.schema.map(sc => {
        when(col(sc.name).cast(DoubleType) >= 1, "1").otherwise("0  ").alias(sc.name)
      }).toArray: _*).select(lit("primaryKeyCandidate").alias(DataSummary.metricColumnName), col("*"))

      val nullCountDf = repartitionDF.select(nullValueCount(repartitionDF.schema): _*)
      val emptyCountDF = repartitionDF.select(emptyCount(repartitionDF.schema): _*)

      val null_value_proportion_df = nullCountDf.select(countColsNullNumber(nullCountDf.schema, total_count): _*).select(lit("nullValueRatio").alias(DataSummary.metricColumnName), col("*"))
      val empty_value_proportion_df = emptyCountDF.select(countColsEmptyNumber(emptyCountDF.columns, total_count): _*).select(lit("blankValueRatio").alias(DataSummary.metricColumnName), col("*"))
      val non_null_df = nullCountDf.join(emptyCountDF).select(repartitionDF.schema.map(sc => {
        lit(total_count) - col("left_" + sc.name) - col("right_" + sc.name)
      }).toArray: _*).select(lit("nonNullCount").alias(DataSummary.metricColumnName), col("*"))

      //    val non_null_df = repartitionDF.select(countNonNullValue(repartitionDF.schema): _*).select(lit("nonNullCount").alias(DataSummary.metricColumnName), col("*"))


      val mean_df = repartitionDF.select(getMeanValue(repartitionDF.schema): _*).select(lit("mean").alias("metric"), col("*"))
      val stddev_df = repartitionDF.select(countColsStdDevNumber(repartitionDF.schema, numericCols): _*).select(lit("standardDeviation").alias(DataSummary.metricColumnName), col("*"))
      val stderr_df = repartitionDF.select(countColsStdErrNumber(repartitionDF.schema, numericCols): _*).select(lit("standardError").alias(DataSummary.metricColumnName), col("*"))


      val maxvalue_df = repartitionDF.select(getMaxNum(repartitionDF.schema, numericCols): _*).select(lit("max").alias(DataSummary.metricColumnName), col("*"))
      val minvalue_df = repartitionDF.select(getMinNum(repartitionDF.schema, numericCols): _*).select(lit("min").alias(DataSummary.metricColumnName), col("*"))
      val datatypelen_df = repartitionDF.select(getTypeLength(repartitionDF.schema): _*).select(lit("dataLength").alias(DataSummary.metricColumnName), col("*"))
      val datatype_sq = Seq("dataType" +: repartitionDF.schema.map(f => f.dataType.typeName match {
        case "null" => "unknown"
        case _ => f.dataType.typeName
      })).map(Row.fromSeq(_))


      val colunm_idx = Seq("ordinalPosition" +: repartitionDF.columns.map(col_name => String.valueOf(repartitionDF.columns.indexOf(col_name) + 1))).map(Row.fromSeq(_))
      var numeric_metric_df = mode_df
        .union(distinct_proportion_df)
        .union(null_value_proportion_df)
        .union(empty_value_proportion_df)
        .union(mean_df)
        .union(non_null_df)
        .union(stddev_df)
        .union(stderr_df)
        .union(maxvalue_df)
        .union(minvalue_df)
        .union(maxlength_df)
        .union(minlength_df)
      numeric_metric_df = roundNumericCols(numeric_metric_df, round_at)

      val schema = StructType(StructField("metrics", StringType, true) +: df.columns.map(StructField(_, StringType, true)).toSeq)
      val sc = spark.sparkContext.emptyRDD[Row]
      var res = spark.createDataFrame(sc, schema)
      val metric_values = metrics.map("'" + _.stripMargin + "'").mkString(",")

      res = res.union(numeric_metric_df)
        .union(is_primary_key_df)
        .union(datatypelen_df)
        .union(spark.createDataFrame(spark.sparkContext.parallelize(datatype_sq, 1), StructType(datatype_schema)))
        .union(spark.createDataFrame(spark.sparkContext.parallelize(colunm_idx, 1), StructType(datatype_schema)))
        .union(new_quantile_df)

      if (metrics == null || metrics.lengthCompare(0) == 0) {
        res = res.select(col("*"))
      } else {
        res = res.select(col("*")).where(s"${DataSummary.metrics} in (${metric_values})")
      }
      //    res.summary()
      // Transpose
      import spark.implicits._
      val (header, data) = res.collect.map(_.toSeq.toArray).transpose match {
        case Array(h, t@_*) => {
          (h.map(_.toString), t.map(_.map(String.valueOf(_))))
        }
      }
      val rows = res.columns.tail.zip(data).map { case (x, ys) => Row.fromSeq(x +: ys) }
      val transposeSchema = StructType(
        StructField("columnName", StringType) +: header.map(StructField(_, StringType))
      )
      res = spark.createDataFrame(spark.sparkContext.parallelize(rows), transposeSchema)
      res
    } catch {
      case e: Exception => throw e
    } finally {
      repartitionDF.unpersist()
    }
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

object DataSummary {
  val metrics = "metrics"
  val roundAt = "roundAt"
  val approxSwitch = "approxSwitch"
  val metricColumnName = "metric"
  val modeFormat = "modeFormat"
}

object ModeValueFormat {
  val all = "all"
  val empty = "empty"
  val auto = "auto"
}