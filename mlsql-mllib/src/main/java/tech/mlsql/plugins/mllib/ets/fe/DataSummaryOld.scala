package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, expr, first, last, length, lit, map_zip_with, max, min, monotonically_increasing_id, round, row_number, spark_partition_id, stddev, when, window}
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, VarcharType}
import streaming.dsl.ScriptSQLExec

import scala.math._
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.{ETMethod, PREDICT}

import scala.util.Try

class DataSummaryOld(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  var round_at = 2

  def countColsNullNumber(schema: StructType, total_count: Long): Array[Column] = {
    schema.map(sc => {
      // For proportion calculation, we remain 2 more digits. Hence, we need set up round_at + 2
      sc.dataType match {
        case DoubleType => round(count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)) / total_count, round_at + 2).alias(sc.name)
        case FloatType => round(count(when(col(sc.name).isNull || col(sc.name).isNaN, sc.name)) / total_count, round_at + 2).alias(sc.name)
        case _ => round(count(when(col(sc.name).isNull, sc.name)) / total_count, round_at + 2).alias(sc.name)
      }
    }).toArray
  }

  def countColsEmptyNumber(columns: Array[String], total_count: Long): Array[Column] = {
    columns.map(c => {
      round(count(when(col(c) === "", c)) / total_count, round_at + 2).alias(c)
    })
  }

  def countColsStdDevNumber(columns: Array[String], numeric_columns: Array[String]): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        round(stddev(col(c)), round_at).alias(c)
      } else {
        max(lit("")).alias(c)
      }
    })
  }

  def countColsStdErrNumber(columns: Array[String], numeric_columns: Array[String], total_count: Long): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        round(stddev(col(c)) / sqrt(total_count), round_at).alias(c)
      } else {
        max(lit("")).alias(c)
      }
    })
  }

  def isPrimaryKey(columns: Array[String], numeric_columns: Array[String], total_count: Long): Array[Column] = {
    columns.map(c => {
      when(countDistinct(col(c)) / total_count === 1, 1).otherwise(0).alias(c)
    })
  }

  def getMaxNum(columns: Array[String], numeric_columns: Array[String]): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        max(col(c)).cast(StringType).alias(c)
      } else {
        //        max(lit("")).alias(c)
        max(col(c)).cast(StringType).alias(c)
      }
    })
  }

  def getMinNum(columns: Array[String], numeric_columns: Array[String]): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        min(col(c)).cast(StringType).alias(c)
      } else {
        min(col(c)).cast(StringType).alias(c)
      }
    })
  }

  def roundAtSingleCol(sc: StructField, column: Column): Column = {
    sc.dataType match {
      case DoubleType => round(column, round_at).cast(StringType)
      case FloatType => round(column, round_at).cast(StringType)
      case _ => column.cast(StringType)
    }
  }

  def getModeNum(schema: StructType, numeric_columns: Array[String], df: DataFrame, round_at: Integer): Array[Column] = {
    schema.map(sc => {
      val dfWithoutNa = df.select(col(sc.name)).na.drop()
      if (dfWithoutNa.isEmpty) {
        /* If no alias is set, multiple columns with null indicators will result in the following error:
        org.apache.spark.sql.AnalysisException: Reference 'first()' is ambiguous, could be: first(), first(). */
        first(lit("")).alias(sc.name)
      } else {
        val mode = dfWithoutNa.groupBy(col(sc.name)).count().orderBy(F.desc("count")).first().get(0)
        roundAtSingleCol(sc, max(lit(mode))).alias(sc.name)
      }
    }).toArray
  }

  def countNonNullValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      count(sc.name).cast(StringType)
    }).toArray
  }

  def getMaxLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case StringType => max(length(col(sc.name)))
        case _ => max(lit(0)).alias(sc.name)
      }
    }).toArray
  }

  def getMinLength(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case StringType => min(length(col(sc.name)))
        case _ => min(lit(0)).alias(sc.name)
      }
    }).toArray
  }


  def getMeanValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      val new_col = sc.dataType match {
        case IntegerType => round(avg(col(sc.name)).cast(DoubleType), round_at)
        case DoubleType => round(avg(col(sc.name)).cast(DoubleType), round_at)
        case FloatType => round(avg(col(sc.name)).cast(DoubleType), round_at)
        case LongType => round(avg(col(sc.name)).cast(DoubleType), round_at)
        case _ => last(lit("")).alias(sc.name)
      }
      new_col
    }).toArray
  }

  def calQuantileValue(lower_value: Double, upper_value: Double, pos: Int): Unit = {

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
    if (c == 1) r.first()
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
          index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
        }
      }
    }
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
        if (!data.isEmpty()) {
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
      sc.dataType match {
        case org.apache.spark.sql.types.ByteType => first(lit(1L)).alias(sc.name)
        case ShortType => first(lit(2L)).alias(sc.name)
        case IntegerType => first(lit(4L)).alias(sc.name)
        case LongType => first(lit(8L)).alias(sc.name)
        case FloatType => first(lit(4L)).alias(sc.name)
        case DoubleType => first(lit(8L)).alias(sc.name)
        case StringType => max(length(col(sc.name))).alias(sc.name)
        case org.apache.spark.sql.types.DateType => first(lit(8L)).alias(sc.name)
        case TimestampType => first(lit(8L)).alias(sc.name)
        case BooleanType => first(lit(1L)).alias(sc.name)
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
    var metrics = params.getOrElse(DataSummary.metrics, "").split(",").filter(!_.equalsIgnoreCase(""))

    val columns = df.columns
    columns.map(col => {
      if (col.contains(".") || col.contains("`")) {
        throw new RuntimeException(s"The column name : ${col} contains special symbols, like . or `, please rename it first!! ")
      }
    })

    val numeric_columns = df.schema.filter(sc => {
      sc.dataType match {
        case IntegerType => true
        case ShortType => true
        case DoubleType => true
        case FloatType => true
        case LongType => true
        case _ => false
      }
    }).map(sc => {
      sc.name
    }).toArray

    val datatype_schema = ("DataType" +: df.schema.map(f => f.dataType.typeName)).map(t => {
      StructField("col_" + t, StringType)
    })


    // get the quantile number for the numeric columns
    val spark = df.sparkSession
    val total_count = df.count()

    var new_quantile_rows = approxSwitch match {
      case true => {
        df.select(df.schema.map(sc => sc.dataType match {
          case IntegerType => col(sc.name)
          case DoubleType => col(sc.name)
          case ShortType => col(sc.name)
          case FloatType => col(sc.name)
          case LongType => col(sc.name)
          case _ => lit(0.0).as(sc.name)
        }): _*).na.fill(0.0).stat.approxQuantile(df.columns, Array(0.25, 0.5, 0.75), 0.05).transpose.map(_.map(String.valueOf(_)).toSeq).map(row =>
          "Q" +: row
        )
      }
      case false => {
        getQuantileNum(df.schema, df, numeric_columns).transpose.map(_.map(v =>
          String.valueOf(v) match {
            case "NaN" => ""
            case _ => String.valueOf(v)
          }
        ).toSeq).map(row =>
          "Q" +: row
        )
      }
    }
    new_quantile_rows = new_quantile_rows.updated(0, new_quantile_rows(0).updated(0, "%25"))
    new_quantile_rows = new_quantile_rows.updated(1, new_quantile_rows(1).updated(0, "median"))
    new_quantile_rows = new_quantile_rows.updated(2, new_quantile_rows(2).updated(0, "%75"))
    var quantile_df_tmp = new_quantile_rows.map(Row.fromSeq(_)).toSeq
    val quantile_df = spark.createDataFrame(spark.sparkContext.parallelize(quantile_df_tmp, 1), StructType(datatype_schema)).na.fill("")

    // If the approx switch is turn on, then calculate the distinct value by approx_count_distinct function,
    // otherwise count distinct by default.

    val distinctvalDF = approxSwitch match {
      case true => {
        val distinctValexprs = df.columns.map((_ -> "approx_count_distinct")).toMap
        df.agg(distinctValexprs)
      }
      case false => df.select(df.columns.map(c => countDistinct(col(c)).alias(c)): _*)
    }

    var mode_df = df.select(getModeNum(df.schema, numeric_columns, df, round_at): _*).select(lit("mode").alias("metric"), col("*"))
    val maxlength_df = df.select(getMaxLength(df.schema): _*).select(lit("maximumLength").alias("metric"), col("*"))
    val minlength_df = df.select(getMinLength(df.schema): _*).select(lit("minimumLength").alias("metric"), col("*"))


    var distinct_proportion_df = distinctvalDF.select(distinctvalDF.columns.map(c => {
      round(col(c) / total_count, round_at + 2)
    }): _*).select(lit("uniqueValueRatio").alias("metric"), col("*"))

    val is_primary_key_df = df.select(isPrimaryKey(df.columns, numeric_columns, total_count): _*).select(lit("primaryKeyCandidate").alias("metric"), col("*"))
    var null_value_proportion_df = df.select(countColsNullNumber(df.schema, total_count): _*).select(lit("nullValueRatio").alias("metric"), col("*"))
    var empty_value_proportion_df = df.select(countColsEmptyNumber(df.columns, total_count): _*).select(lit("blankValueRatio").alias("metric"), col("*"))
    var mean_df = df.select(getMeanValue(df.schema): _*).select(lit("mean").alias("metric"), col("*"))
    var stddev_df = df.select(countColsStdDevNumber(df.columns, numeric_columns): _*).select(lit("standardDeviation").alias("metric"), col("*"))
    var stderr_df = df.select(countColsStdErrNumber(df.columns, numeric_columns, total_count): _*).select(lit("standardError").alias("metric"), col("*"))
    var non_null_df = df.select(countNonNullValue(df.schema): _*).select(lit("nonNullCount").alias("metric"), col("*"))
    val maxvalue_df = df.select(getMaxNum(df.columns, numeric_columns): _*).select(lit("max").alias("metric"), col("*"))
    val minvalue_df = df.select(getMinNum(df.columns, numeric_columns): _*).select(lit("min").alias("metric"), col("*"))
    val datatypelen_df = df.select(getTypeLength(df.schema): _*).select(lit("dataLength").alias("metric"), col("*"))
    val datatype_sq = Seq("dataType" +: df.schema.map(f => f.dataType.typeName)).map(Row.fromSeq(_))


    val colunm_idx = Seq("ordinalPosition" +: df.columns.map(col_name => String.valueOf(df.columns.indexOf(col_name) + 1))).map(Row.fromSeq(_))
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
      .union(quantile_df)

    if (metrics == null || metrics.length == 0) {
      res = res.select(col("*"))
    } else {
      res.select(col("*")).where(s"${DataSummary.metrics} in (${metric_values})")
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

object DataSummaryOld {
  val metrics = "metrics"
  val roundAt = "roundAt"
  val approxSwitch = "approxSwitch"
}
