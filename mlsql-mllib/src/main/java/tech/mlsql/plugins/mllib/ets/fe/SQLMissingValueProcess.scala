package tech.mlsql.plugins.mllib.ets.fe


import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.{DiscretizerFeature, VectorAssembler}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

class SQLMissingValueProcess(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  private def fillByRandomForestRegressor(df: DataFrame, params: Map[String, String], processColArrays: Array[String]): DataFrame = {
    val regressor = new RandomForestRegressor()
    configureModel(regressor, params)
    val fullColumns = df.columns
    var data = df
    processColArrays.foreach(processCol => {
      val featureCols = fullColumns.filter(c => {
        c != processCol
      })
      val unknown_data = data.filter(col(processCol).isNull)
      val known_data = data.filter(col(processCol).isNotNull)
      val featureName = "features"
      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol(featureName)
      val trainData = assembler.transform(known_data)
      val testData = assembler.transform(unknown_data)
      val regressor = new RandomForestRegressor()
        .setLabelCol(processCol)
        .setFeaturesCol(featureName)
        .setPredictionCol("predicted")
      val model = regressor.fit(trainData)
      val predictedData = model.transform(testData)
      // Fillout the misisng value by the predict
      val result = predictedData.withColumn(processCol,
        when(col(processCol).isNull, col("predicted")).otherwise(col(processCol))
      ).drop("predicted").drop("features")
      data = known_data.union(result)
    })
    data
  }

  private def dropWithMissingValue(df: DataFrame, params: Map[String, String]): DataFrame = {
    var data = df
    val columns = df.columns
    columns.foreach(colName => {
      data = data.filter(col(colName).isNotNull)
    })
    data
  }

  private def fillByMeanValue(df: DataFrame, params: Map[String, String], processColumns: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    var result = df
    processColumns.foreach(colName => {
      val meanValue = df.agg(mean(col(colName)).alias("mean")).collect()(0).get(0)
      result = result.withColumn(colName, when(col(colName).isNull, meanValue).otherwise(col(colName)))
    })
    result
  }

  private def fillByModeValue(df: DataFrame, params: Map[String, String], processColumns: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    var result = df
    processColumns.foreach(colName => {
      val modeValue = df.select(colName).groupBy(colName).count().orderBy(df(colName).desc).collect()(0).get(0)
      result = result.withColumn(colName, when(col(colName).isNull, modeValue).otherwise(col(colName)))
    })
    result
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val processColumns = params.getOrElse(processColumnsParam.name, "").split(",")
    val method = params.getOrElse(methodParam.name, "mean")
    val data = method match {
      case "mean" => fillByMeanValue(df, params, processColumns)
      case "mode" => fillByModeValue(df, params, processColumns)
      case "drop" => dropWithMissingValue(df, params)
      case "randomforest" => fillByRandomForestRegressor(df, params, processColumns)
    }
    data
  }

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |set abc='''
      |{"name": "elena", "age": 57, "phone": 15552231521, "income": 433000, "label": 0}
      |{"name": "candy", "age": 67, "phone": 15552231521, "income": null, "label": 0}
      |{"name": "bob", "age": 57, "phone": 15252211521, "income": 89000, "label": 0}
      |{"name": "candy", "age": 25, "phone": 15552211522, "income": 36000, "label": 1}
      |{"name": "candy", "age": 31, "phone": 15552211521, "income": null, "label": 1}
      |{"name": "finn", "age": 23, "phone": 15552211521, "income": 238000, "label": 1}
      |''';
      |load jsonStr.`abc` as table1;
      |run table1 as MissingValueProcess.`/tmp/fe_test/missingvalueFillout`
      |where method="mean"
      |and processColumns='income';
      |
      |;
    """.stripMargin)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_missingvalue_process_operator__"),
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

  val processColumnsParam: Param[String] = new Param[String](this, MissingValueProcessConstant.PORCOESS_COLUMNS,
    FormParams.toJson(Text(
      name = MissingValueProcessConstant.PORCOESS_COLUMNS,
      value = "",
      extra = Extra(
        doc =
          """
            | The selected columns that are going to be processed!
          """,
        label = "The selected column name",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val methodParam: Param[String] = new Param[String](this, MissingValueProcessConstant.METHOD, FormParams.toJson(
    Select(
      name = MissingValueProcessConstant.METHOD,
      values = List(),
      extra = Extra(
        doc = "",
        label = "",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
        )), valueProvider = Option(() => {
        List(
          KV(Some("method"), Some(MissingValueProcessConstant.PROCESS_BY_MEAN)),
          KV(Some("method"), Some(MissingValueProcessConstant.PROCESS_BY_MODE)),
          KV(Some("method"), Some(MissingValueProcessConstant.PROCESS_BY_DROP)),
          KV(Some("method"), Some(MissingValueProcessConstant.PROCESS_BY_RF))
        )
      })
    )
  ))


}

object MissingValueProcessConstant {
  val METHOD = "method"

  val PORCOESS_COLUMNS = "processColumns"

  val PROCESS_BY_MEAN = "mean"

  val PROCESS_BY_MODE = "mode"

  val PROCESS_BY_DROP = "drop"

  val PROCESS_BY_RF = "randomforest"


}