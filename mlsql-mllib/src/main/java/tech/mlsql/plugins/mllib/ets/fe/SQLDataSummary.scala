package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, bround, col, count, countDistinct, expr, first, grouping, length, lit, max, min, round, stddev, when}
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, VarcharType}
import streaming.dsl.ScriptSQLExec

import scala.math._
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.{ETMethod, PREDICT}

class SQLDataSummary(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  def countColsNullNumber(columns: Array[String], total_count: Long, round_at: Integer): Array[Column] = {
    columns.map(c => {
      round(count(when(col(c).isNull || col(c).isNaN, c)) / total_count, round_at).alias(c)
    })
  }

  def countColsEmptyNumber(columns: Array[String], total_count: Long, round_at: Integer): Array[Column] = {
    columns.map(c => {
      round(count(when(col(c) === "", c)) / total_count, round_at).alias(c)
    })
  }

  def countColsStdDevNumber(columns: Array[String], numeric_columns: Array[String], round_at: Integer): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        round(stddev(col(c)), round_at).alias(c)
      } else {
        lit("").alias(c)
      }
    })
  }

  def countColsStdErrNumber(columns: Array[String], numeric_columns: Array[String], total_count: Long, round_at: Integer): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        round(stddev(col(c)) / sqrt(total_count), round_at).alias(c)
      } else {
        lit("").alias(c)
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
        (max(col(c))).alias(c)
      } else {
        lit("").alias(c)
      }
    })
  }

  def getMinNum(columns: Array[String], numeric_columns: Array[String]): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        (min(col(c))).alias(c)
      } else {
        lit("").alias(c)
      }
    })
  }


  def getModeNum(columns: Array[String], numeric_columns: Array[String], df: DataFrame): Array[Column] = {
    columns.map(c => {
      if (numeric_columns.contains(c)) {
        val mode = df.groupBy(col(c)).count().orderBy(F.desc("count")).first().get(0)
        max(lit(mode)).alias(c)
      } else {
        max(lit("")).alias(c)
      }
    })
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

  def countNonNullValue(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      count(c).alias(c)
    })
  }

  def getMeanValue(schema: StructType): Array[Column] = {
    schema.map(sc => {
      sc.dataType match {
        case IntegerType => avg(col(sc.name))
        case DoubleType => avg(col(sc.name))
        case FloatType => avg(col(sc.name))
        case LongType => avg(col(sc.name))
        case _ => min(lit(0)).alias(sc.name)
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
        case StringType => first(lit(-2L)).alias(sc.name)
        case org.apache.spark.sql.types.DateType => first(lit(8L)).alias(sc.name)
        case TimestampType => first(lit(8L)).alias(sc.name)
        case BooleanType => first(lit(1L)).alias(sc.name)
        case _ => first(lit(-1L)).alias(sc.name)
      }
    }).toArray
  }

  def roundNumericCols(df: DataFrame, round_at: Integer): DataFrame = {
    df.select(df.schema.map(sc => {
      sc.dataType match {
        case IntegerType => expr(s"cast (${sc.name} as decimal(38,2)) as ${sc.name}")
        case DoubleType => expr(s"cast (${sc.name} as decimal(38,2)) as ${sc.name}")
        case FloatType => expr(s"cast (${sc.name} as decimal(38,2)) as ${sc.name}")
        case LongType => expr(s"cast (${sc.name} as decimal(38,2)) as ${sc.name}")
        case _ => col(sc.name)
      }
    }): _*)
  }

  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val round_at = Integer.valueOf(params.getOrElse("roundAt", "2"))
    var metrics = params.getOrElse(DataSummary.metrics, "").split(",")

    val columns = df.columns
    columns.map(col => {
      if (col.contains(".") || col.contains("`")) {
        throw new RuntimeException(s"The column name : ${col} contains special symbols, like . or `, please rename it first!! ")
      }
    })

    val numeric_columns = df.schema.filter(sc => {
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
    var new_df = df.select(numeric_columns.head, numeric_columns.tail: _*)
    // get the quantile number for the numeric columns
    val spark = df.sparkSession
    val total_count = df.count()
    var new_quantile_rows = df.select(df.schema.map(sc => sc.dataType match {
      case IntegerType => col(sc.name)
      case DoubleType => col(sc.name)
      case FloatType => col(sc.name)
      case LongType => col(sc.name)
      case _ => lit(0.0).as(sc.name)
    }): _*).stat.approxQuantile(df.columns, Array(0.25, 0.5, 0.75), 0.05).transpose.map(_.map(String.valueOf(_)).toSeq).map(row =>
      "Q" +: row
    )
    new_quantile_rows = new_quantile_rows.updated(0, new_quantile_rows(0).updated(0, "%25"))
    new_quantile_rows = new_quantile_rows.updated(1, new_quantile_rows(1).updated(0, "median"))
    new_quantile_rows = new_quantile_rows.updated(2, new_quantile_rows(2).updated(0, "%75"))
    var new_quantile_df = new_quantile_rows.map(Row.fromSeq(_)).toSeq
    var mode_df = df.select(getModeNum(df.columns, numeric_columns, df): _*).select(lit("mode").alias("metric"), col("*"))
    val quantileNum = new_df.stat.approxQuantile(numeric_columns, Array(0.25, 0.5, 0.75), 0.05)
    val maxlength_df = df.select(getMaxLength(df.schema): _*).select(lit("maximumLength").alias("metric"), col("*"))
    val minlength_df = df.select(getMinLength(df.schema): _*).select(lit("minimumLength").alias("metric"), col("*"))

    val distinctValexprs = df.columns.map((_ -> "approx_count_distinct")).toMap
    val distinctvalDF = df.agg(distinctValexprs)
    var distinct_proportion_df = distinctvalDF.select(distinctvalDF.columns.map(c => {
      round(col(c) / total_count, round_at)
    }): _*).select(lit("uniqueValueRatio").alias("metric"), col("*"))
    val is_primary_key_df = df.select(isPrimaryKey(df.columns, numeric_columns, total_count): _*).select(lit("primaryKeyCandidate").alias("metric"), col("*"))
    var null_value_proportion_df = df.select(countColsNullNumber(df.columns, total_count, round_at): _*).select(lit("nullValueRatio").alias("metric"), col("*"))
    var empty_value_proportion_df = df.select(countColsEmptyNumber(df.columns, total_count, round_at): _*).select(lit("blankValueRatio").alias("metric"), col("*"))
    var mean_df = df.select(getMeanValue(df.schema): _*).select(lit("mean").alias("metric"), col("*"))
    var stddev_df = df.select(countColsStdDevNumber(df.columns, numeric_columns, round_at): _*).select(lit("standardDeviation").alias("metric"), col("*"))
    var stderr_df = df.select(countColsStdErrNumber(df.columns, numeric_columns, total_count, round_at): _*).select(lit("standardError").alias("metric"), col("*"))
    var non_null_df = df.select(countNonNullValue(df.columns): _*).select(lit("nonNullCount").alias("metric"), col("*"))

    val maxvalue_df = df.select(getMaxNum(df.columns, numeric_columns): _*).select(lit("max").alias("metric"), col("*"))
    val minvalue_df = df.select(getMinNum(df.columns, numeric_columns): _*).select(lit("min").alias("metric"), col("*"))
    val datatypelen_df = df.select(getTypeLength(df.schema): _*).select(lit("dataLength").alias("metric"), col("*"))
    val datatype_sq = Seq("dataType" +: df.schema.map(f => f.dataType.typeName)).map(Row.fromSeq(_))
    val datatype_schema = ("DataType" +: df.schema.map(f => f.dataType.typeName)).map(t => {
      StructField("col_" + t, StringType)
    })
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
      .union(spark.createDataFrame(spark.sparkContext.parallelize(new_quantile_df, 1), StructType(datatype_schema)))
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
}