package tech.mlsql.plugins.mllib.ets.fintech.scorecard

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Bucketizer, DiscretizerFeature, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{bin, col, lit, udf, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions, SQLPythonFunc}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/1/4 16:36
 *
 */
class SQLScoreCard(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  def constructBinningMap(binningDF: DataFrame): Map[String, Map[Double, Double]] = {
    binningDF.rdd.collect().map(row => {
      val featureName = row.get(0).toString
      val binStr = row.get(1).toString
      val bin = JSON.parseFull(binStr)
      var binMap = bin match {
        case Some(map: collection.immutable.Map[String, Any]) => map
      }
      val binToWoe = binMap.get("bin").get.asInstanceOf[List[Map[String, Any]]].map(b => {
        val woe = b.get("woe").get.asInstanceOf[Double]
        val binid = b.get("bin_id").get.asInstanceOf[Double]
        (binid, woe)
      }).toMap
      (featureName, binToWoe)
    }).toMap
  }

  // 1. replace the feature with woe value [Training Data + Bining Data]
  def replaceFeatureWithWOE(trainDF: DataFrame, binningDF: DataFrame, params: Map[String, String]): DataFrame = {
    //  The first table is the table after binning with binning id
    //  The second table is the binning information table, including bin id and relative woe/iv values
    //  val binningMap: Map[String, Map[Int, Double]] =

    val binningMap = constructBinningMap(binningDF)
    var trainDFTmp = trainDF
    params.getOrElse(selectedFeaturesParam.name, "").split(",").map(
      f => {
        binningMap.get(f).get.map(bm => {
          val woe = bm._2
          trainDFTmp = trainDFTmp.withColumn(f + "_output", when(col(f + "_output") === bm._1.asInstanceOf[Double], woe).otherwise(col(f + "_output")))
        })
      }
    )
    val selectedFeatures = params.getOrElse(selectedFeaturesParam.name, "").split(",")
    selectedFeatures.map(
      f => {
        trainDFTmp = trainDFTmp.drop(f).withColumnRenamed(f + "_output", f)
      }
    )
    val assembler = new VectorAssembler().setInputCols(selectedFeatures).setOutputCol("features")
    assembler.transform(trainDFTmp)
  }

  // 2. Training with LogisticRegression  [Training data]
  def scorecardTraining[T <: Model[T]](df: DataFrame, params: Map[String, String]): LogisticRegressionModel = {
    val lr = new LogisticRegression().setLabelCol("label")
      .setFeaturesCol("features")
      .setFitIntercept(true)

    val model = lr.fit(df)
    val path = params.get("path").get
    val modelPath = SQLPythonFunc.getAlgModelPath(path) + "/" + 0
    model.asInstanceOf[MLWritable].write.overwrite().save(modelPath)
    model
  }

  // 3. prediction
  def scorecardPrediction(df: DataFrame, model: LogisticRegressionModel): DataFrame = {
    model.transform(df)
  }

  // 4. transform prod to score
  def transformProdToScore(transformedDF: DataFrame, goodValue: String, pdo: Long, scaledValue: Long, odds: Long): DataFrame = {
    val score_trans_udf = udf((probability: Array[Double], goodValue: String,
                               pdo: Long, scaledValue: Long, odd: Long) => {
      val p_good = goodValue.toInt match {
        case 0 => probability(0)
        case 1 => probability(1)
      }
      val p_bad = 1.0 - p_good
      val a = Math.log(p_good / p_bad)
      val factor = pdo.toDouble / Math.log(2)
      val offset = scaledValue.toDouble - pdo.toDouble * ((Math.log(odd)) / (Math.log(2)))
      val score = offset + factor * a
      score
    })
    val convert = udf((col: DenseVector) => {
      col.toArray
    })

    transformedDF.withColumn("probability", convert(col("probability")))
      .withColumn("predictedScore", score_trans_udf(col("probability"), lit(goodValue), lit(pdo), lit(scaledValue), lit(odds)))
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    // the data in the table df is expected to being processed by binning ET
    val binningInfoTableName = params.getOrElse(binningTableParam.name, "")
    require(!binningInfoTableName.isEmpty, "The binning information table is required! " +
      "Please specify the binning info table name")

    val goodValue = params.getOrElse(goodValueParam.name, "1")
    val odds = params.getOrElse(oddValueParam.name, "50").toInt
    val pdo = params.getOrElse(pdoValueParam.name, "25").toInt
    val scaledValue = params.getOrElse(scaledValueParam.name, "800").toInt

    ScoreCard.GOOD_VALUE = goodValue
    ScoreCard.ODDS = odds
    ScoreCard.PDO = pdo
    ScoreCard.SCALE_VALUE = scaledValue

    val binningAlg = MLMapping.findAlg("Binning")
    val dfAfterBinning = binningAlg.batchPredict(df, path, Map())
    val binningInfoTable = spark.table(binningInfoTableName)
    val dfWithWoe = replaceFeatureWithWOE(dfAfterBinning, binningInfoTable, params)

    val scorecardPath = ScoreCard.generateScorecardPath(path)
    val updatedParams = params ++ Map("path" -> scorecardPath)
    val scorecardModel = scorecardTraining(dfWithWoe, updatedParams).asInstanceOf[LogisticRegressionModel]
    val pred = scorecardPrediction(dfWithWoe, scorecardModel)
    transformProdToScore(pred, goodValue, pdo, scaledValue, odds)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    //1. load LogisticRegression/LinearRgression model
    val scoreCardPath = ScoreCard.generateScorecardPath(path)
    val modelPath = SQLPythonFunc.getAlgModelPath(scoreCardPath) + "/" + 0
    val lrModel = LogisticRegressionModel.load(modelPath)

    //2. load Discretizer model
    val binningAlg = MLMapping.findAlg("Binning")
    val (stringCols, labelArrays, bucketizer, dTypeCols) = binningAlg.load(sparkSession, path, params).asInstanceOf[(Array[String], Array[Array[String]], Bucketizer, Array[String])]

    val binningModel = BinningTrainData(
      bucketizer.getInputCols,
      bucketizer.getOutputCols,
      false,
      bucketizer.getSplitsArray,
      stringCols,
      labelArrays,
      dTypeCols)

    //3. read binning info table
    val binningInfoTableName = params.getOrElse(binningTableParam.name, "")
    require(!binningInfoTableName.isEmpty, "The binning information table is required! " +
      "Please specify the binning info table name")
    val binningInfoTable = sparkSession.table(binningInfoTableName)

    (lrModel, binningModel, binningInfoTable)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val meta = sparkSession.sparkContext.broadcast(_model.asInstanceOf[(LogisticRegressionModel, BinningTrainData, DataFrame)])
    val (model, binningModel, binningDF) = meta.value
    val binningMap = constructBinningMap(binningDF).toArray
    //    val transformer = (features: Array[String]) => {
    val transformer: Seq[String] => AnyRef = features => {
      var strIdx = 0
      var numIdx = 0
      val featureWithWOE = features.zipWithIndex.map {
        case (feature, index) =>
          val binid = binningModel.dTypeCols(index) match {
            case "String" =>
              val labelIndex = binningModel.stringLabelsArray(strIdx)
              strIdx += 1
              DiscretizerFeature.strColIndexSearch(labelIndex, feature)
            case "Integer" | "Double" =>
              val splits = binningModel.splitsArray(numIdx)
              numIdx += 1
              DiscretizerFeature.binarySearchForBuckets(splits, feature.toDouble, false)
          }
          val woe = binningMap(index)._2.get(binid).get
          (index, woe)
      }
      val predData = Vectors.sparse(featureWithWOE.size, featureWithWOE)
      val result = model.getClass.getMethod("predict", classOf[Vector]).invoke(model, predData)
      result
    }
    MLSQLUtils.createUserDefinedFunction(transformer, DoubleType, Some(Seq(VectorType)))
  }

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |set abc='''
      |{"name": "elena", "age": 57, "phone": 15552231521, "income": 433000, "label": 0}
      |{"name": "candy", "age": 67, "phone": 15552231521, "income": 190000, "label": 0}
      |{"name": "bob", "age": 57, "phone": 15252211521, "income": 89000, "label": 0}
      |{"name": "candy", "age": 25, "phone": 15552211522, "income": 36000, "label": 1}
      |{"name": "candy", "age": 31, "phone": 15552211521, "income": 299000, "label": 1}
      |{"name": "finn", "age": 23, "phone": 15552211521, "income": 238000, "label": 1}
      |''';
      |load jsonStr.`abc` as table1;
      |
      |run table1 as Binning.`/tmp/fintech` where
      |label='label' and method='EF'
      |and numBucket='3'
      |and selectedFeatures="name,age,income"
      |as binningTestTable;
      |
      |run table1 as ScoreCard.`/tmp/fintech`
      |where binningTable="binningTestTable"
      |and selectedFeatures='name,age,income'
      |and scaledValue='900'
      |and odds='60'
      |and pdo='10'
      |as scorecardTable;
      |
      |predict table1 as ScoreCard.`/tmp/fintech`
      |where binningPath='/tmp/fe_test/binning'
      |and binningTable='binningTestTable'
      |and selectedFeatures='name,age,income'
      |as predictedScoreCardTable;
      |
      |;
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val (lrModel, binningModel, binningInfoTable) = load(df.sparkSession, path, params).asInstanceOf[(LogisticRegressionModel, BinningTrainData, DataFrame)]
    val dataAfterDiscretizer = MLMapping.findAlg("Binning").batchPredict(df, path, Map())
    val dataWithWOE = replaceFeatureWithWOE(dataAfterDiscretizer, binningInfoTable, params)
    val scorecardPred = scorecardPrediction(dataWithWOE, lrModel)
    transformProdToScore(scorecardPred, ScoreCard.GOOD_VALUE, ScoreCard.PDO, ScoreCard.SCALE_VALUE, ScoreCard.ODDS)
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_scorecard_operator__"),
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

  val selectedFeaturesParam: Param[String] = new Param[String](this, Binning.SELECTED_FEATURES,
    FormParams.toJson(Text(
      name = Binning.SELECTED_FEATURES,
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

  val binningTableParam: Param[String] = new Param[String](this, ScoreCard.BINNING_TABLE,
    FormParams.toJson(Text(
      name = ScoreCard.BINNING_TABLE,
      value = "",
      extra = Extra(
        doc =
          """
            | The binning table name for scorecard training
          """,
        label = "The binning table name from previous step!",
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

  val scaledValueParam: Param[String] = new Param[String](this, ScoreCard.SCALE_VALUE_NAME,
    FormParams.toJson(Text(
      name = ScoreCard.SCALE_VALUE_NAME,
      value = "",
      extra = Extra(
        doc =
          """
            | The scaled value for transforming to credit score
          """,
        label = "The scaled value for transforming to credit score!",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val pdoValueParam: Param[String] = new Param[String](this, ScoreCard.PDO_NAME,
    FormParams.toJson(Text(
      name = ScoreCard.PDO_NAME,
      value = "",
      extra = Extra(
        doc =
          """
            | The PDO value for transforming to credit score
          """,
        label = "The PDO value for transforming to credit score!",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val oddValueParam: Param[String] = new Param[String](this, ScoreCard.ODDS_NAME,
    FormParams.toJson(Text(
      name = ScoreCard.ODDS_NAME,
      value = "",
      extra = Extra(
        doc =
          """
            | The ODD value for transforming to credit score
          """,
        label = "The ODD value for transforming to credit score!",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val goodValueParam: Param[String] = new Param[String](this, ScoreCard.GOOD_VALUE_NAME,
    FormParams.toJson(Text(
      name = ScoreCard.GOOD_VALUE_NAME,
      value = "",
      extra = Extra(
        doc =
          """
            | The value that indicated the good user class, default with 1
          """,
        label = "The value that indicated the good user class!",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
}

object ScoreCard {
  var SCALE_VALUE = 800
  var ODDS = 50
  var PDO = 25
  var GOOD_VALUE = "1"
  val SCALE_VALUE_NAME = "scaledValue"
  val ODDS_NAME = "odds"
  val PDO_NAME = "pdo"
  val GOOD_VALUE_NAME = "goodValue"
  val BINNING_TABLE = "binningTable"
  val SELECTED_FEATURES = "selectedFeatures"

  def generateScorecardPath(path: String): String = {
    s"${path.stripSuffix("/")}/scorecard"
  }
}
