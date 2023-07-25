package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.ml.stat.KolmogorovSmirnovTest
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, countDistinct, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib._
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.mllib.ets.PluginBaseETAuth


/**
 * 4/1/2023 ff(fflxm@outlook.com)
 */
class SQLColumnsAnalysis(override val uid: String) extends SQLAlg
  with Functions
  with MllibFunctions
  with BaseClassification
  with PluginBaseETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    if (df.isEmpty){
      return df.sparkSession.emptyDataFrame
    }

    val _action = params.getOrElse(action.name, $(action).toString)
    val _target = params.getOrElse(target.name, $(target).toString)
    val _positive = params.getOrElse(positive.name, $(positive).toString)
    val _fields = params.getOrElse(fields.name, $(fields).mkString(",")).split(",")
    if (_fields.length == 0) return df

    val dfName = params("__dfname__")
    val data = _action match {
      case "KS" => ks(df, _fields, _target)
      case "IV" => iv(df, _fields, _target, _positive.toDouble)
      case "P_VALUE" => pvalue(df, _fields, _target)
      case "MISS_RATE" => lack(df, _fields, _target)
      case "HORIZONTAL_SCORE" => horizontal(df, _fields, _target)
      case "PEARSON_IV" => person(df, _fields, _target)
      case "PEARSON_DV" => person(df, _fields, _target)
    }
    data
  }

  private def ks(df: DataFrame, fields: Array[String], target:String): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    fields.foreach(colName => {
      val ff = df.withColumn(colName, col(colName).cast(DoubleType))

      val Row(pValue: Double, statistic: Double) = KolmogorovSmirnovTest
        .test(ff, colName, "norm", 0, 1).head()
      seqresult += (colName->statistic)
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def iv(df: DataFrame, fields: Array[String], target:String, positive:Double): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    fields.foreach(colName => {
      //val ff = df.withColumn(colName, col(colName).cast(DoubleType))
      val ff = df.withColumn(target, when(col(colName).isNotNull.cast(DoubleType) === positive ,1).otherwise(0))

      val statistic = IV(ff,colName, target)
      seqresult += (colName->statistic)
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def pvalue(df: DataFrame, fields: Array[String], target:String): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    fields.foreach(colName => {
      val ff = df.withColumn(colName, col(colName).cast(DoubleType))

      val Row(pValue: Double, statistic: Double) = KolmogorovSmirnovTest
        .test(ff, colName, "norm", 0, 1).head()
      seqresult += (colName->pValue)
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def lack(df: DataFrame, fields: Array[String], target:String): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    fields.foreach(colName => {
      val miss_cnt=df.select(col(colName)).where(col(colName).isNull).count
      //统计每列的缺失率，并保留4位小数
      val statistic=(miss_cnt/df.count()).toDouble.formatted("%.4f").toDouble

      seqresult += (colName->statistic)
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def horizontal(df: DataFrame, fields: Array[String], target:String): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    fields.foreach(colName => {
      val statistic= df.agg(countDistinct(colName)).collect().map(_(0)).toList(0).toString.toDouble
      seqresult += (colName->statistic)
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  private def person(df: DataFrame, fields: Array[String], target:String): DataFrame = {
    var seqresult:Map[String , Double] = Map();
    fields.foreach(colName => {
      //val Row(coeff2: Matrix) = Correlation.corr(df,colName).head()
      val pearsonValue = PEARSON(df, colName, target)
      seqresult += (colName->pearsonValue)
    })
    val spark = df.sparkSession
    spark.createDataFrame(seqresult.toSeq).toDF("colName","statistic")
  }

  override def skipOriginalDFName: Boolean = false

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |
      |""".stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |
      |set abc='''
      |{"name": "elena", "age": 57, "phone": 15552231521, "income": 433000, "label": 0}
      |{"name": "candy", "age": 67, "phone": 15552231521, "income": 1200, "label": 0}
      |{"name": "bob", "age": 57, "phone": 15252211521, "income": 89000, "label": 0}
      |{"name": "candy", "age": 25, "phone": 15552211522, "income": 36000, "label": 1}
      |{"name": "candy", "age": 31, "phone": 15552211521, "income": 300000, "label": 1}
      | {"name": "finn", "age": 23, "phone": 15552211521, "income": 238000, "label": 1}
      |''';
      |
      |load jsonStr.`abc` as table1;
      |run table1 as ColumnsAnalysis.`` where action='iv' and fields='income,age,phone' as outputTable;
      |select colName from outputTable where statistic < 0.2 as t2;
      |""".stripMargin)


  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def etName: String = "__fe_columns_analysis_operator__"

  final val action: Param[String] =
    new Param[String](this, name = "action", doc = "")
  setDefault(action, "KS")

  final val target: Param[String] =
    new Param[String](this, name = "target", doc = "")
  setDefault(target, "null")

  final val positive: Param[String] =
    new Param[String](this, name = "positive", doc = "")
  setDefault(positive, "0")

  final val fields: StringArrayParam =
    new StringArrayParam(this, name = "fields", doc = "")
  setDefault(fields, Array[String]())

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_columns_analysis_operator__"),
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

  private def IV(srcDf: DataFrame, featureCol: String, target: String): Double = {
    var ivvalue: Double = 0.0

    val discretizer = new QuantileDiscretizer()
      .setInputCol(featureCol)
      .setOutputCol("result")
      .setNumBuckets(10)//默认值取10，最好可以外部输入

    val result = discretizer.fit(srcDf).transform(srcDf)
    result.show()
    val pos_all_num = if(result.filter(col(target) ===1).count() <=0) 1 else result.filter(col(target) ===1).count()
    val neg_all_num = if(result.filter(col(target) ===0).count() <=0) 1 else result.filter(col(target) ===0).count()
    val bluk_num = result.selectExpr("result").collect().distinct.length

    var i = 0
    while( i < bluk_num){
      val bluk_group = result.filter(col("result") === i.toDouble)

      val good_group_num = bluk_group.filter(col(target) ===1).count()
      val bad_group_num = bluk_group.filter(col(target) ===0).count()

      val good_group = if(good_group_num != 0) good_group_num else 1
      val bad_group = if(bad_group_num != 0) bad_group_num else 1

      val iv = (good_group.toDouble/pos_all_num.toDouble - bad_group.toDouble/neg_all_num.toDouble) * math.log((good_group.toDouble/pos_all_num.toDouble)/(bad_group.toDouble/neg_all_num.toDouble))
      ivvalue = ivvalue + iv
      i += 1
    }

    ivvalue
  }

  private def PEARSON(srcDf: DataFrame, featureCol: String, targetCol:String): Double = {
    val df_real = srcDf.select(targetCol, featureCol)
    val rdd_real = df_real.rdd.map(x=>(x(0).toString.toDouble ,x(1).toString.toDouble ))
    val label = rdd_real.map(x=>x._1.toDouble )
    val feature = rdd_real.map(x=>x._2.toDouble )

    val cor_pearson:Double = Statistics.corr(feature, label,"pearson")
    cor_pearson
  }
}
