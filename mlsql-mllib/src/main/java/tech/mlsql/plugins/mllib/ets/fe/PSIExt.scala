package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.util.parsing.json.JSON

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/3/7 11:38
 *
 */
class PSIExt(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {

  def this() = this(BaseParams.randomUID())

  def constructBinningMap(binningDF: DataFrame): Map[String, Array[(Double, String)]] = {
    binningDF.rdd.collect().map(row => {
      val featureName = row.get(0).toString
      val binStr = row.get(1).toString
      val bin = JSON.parseFull(binStr)
      var binMap = bin match {
        case Some(map: collection.immutable.Map[String, Any]) => map
      }
      val binIdWithDesc = binMap.get("bin").get.asInstanceOf[List[Map[String, Any]]].map(b => {
        val binid = b.get("bin_id").get.asInstanceOf[Double]
        val bindesc = b.get("value").get.asInstanceOf[String]
        (binid, bindesc)
      }).toArray
      (featureName, binIdWithDesc)
    }).toMap
  }

  def samplePercentageCalculator(feaName: String, binID: Double, bindesc: String, expectedDF: DataFrame, actualDF: DataFrame, expectedTotal: Double, actualTotal: Double):
  (String, Double, String, Double, Double, Double, Double, Double) = {
    val actualAmount = actualDF.select(col(PSIObject.BINNING_RESULT_COLUNM_NAME_CONVERT(feaName))).where(col(PSIObject.BINNING_RESULT_COLUNM_NAME_CONVERT(feaName)) === binID).count().toDouble
    val actualPercentage = actualAmount / actualTotal
    val expectedAmount = expectedDF.select(col(PSIObject.BINNING_RESULT_COLUNM_NAME_CONVERT(feaName))).where(col(PSIObject.BINNING_RESULT_COLUNM_NAME_CONVERT(feaName)) === binID).count().toDouble
    val expectedPercentage = expectedAmount / expectedTotal
    val psi = (actualPercentage - expectedPercentage) * Math.log(actualPercentage / expectedPercentage)
    (feaName, binID, bindesc, actualPercentage, expectedPercentage, actualPercentage - expectedPercentage, Math.log(actualPercentage / expectedPercentage), psi)
  }

  def PSI(expectedDF: DataFrame, actualDF: DataFrame, binningTable: DataFrame, binningPath: String, selectedFratures: Array[String]): DataFrame = {
    val binningAlg = MLMapping.findAlg(PSIObject.BINNING_EXT)
    val transformedExpected = binningAlg.batchPredict(expectedDF, binningPath, Map())
    val transformedActual = binningAlg.batchPredict(actualDF, binningPath, Map())

    val expectedTotal = transformedExpected.count().toDouble
    val actualTotal = transformedActual.count().toDouble

    val binningMap = constructBinningMap(binningTable)
    val finalData = binningMap.map(data => {
      val feaName = data._1
      val binIdArray = data._2
      binIdArray.map(bin => {
        val binid = bin._1
        val bindesc = bin._2
        samplePercentageCalculator(feaName, binid, bindesc, transformedExpected, transformedActual, expectedTotal, actualTotal)
      })
    }).reduce((_1, _2) => _1 ++ _2).toSeq
    val resDF = expectedDF.sparkSession.createDataFrame(finalData)
      .toDF("feature_name", "bin_id", "bin_desc", "actual_percentage",
        "expected_percentage", "actual-expected", "ln(actual/expected)", "psi")
    resDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val expectedTableName = params.getOrElse(expectedTable.name, "")
    val expectedDF = expectedTableName match {
      case "" => df
      case _ => spark.table(expectedTableName)
    }

    val actualTableName = params.getOrElse(actualTable.name, "")
    val actualDF = spark.table(actualTableName)

    val binningTableName = params.getOrElse(binningTable.name, "")
    val binningDF = spark.table(binningTableName)

    val _binningPath = params.getOrElse(binningPath.name, "")

    val _selectedFeatures = params.getOrElse(selectedFeatures.name, "")

    PSI(expectedDF, actualDF, binningDF, _binningPath, _selectedFeatures.split(","))
  }

  final val expectedTable: Param[String] = new Param[String](parent = this
    , name = "expectedTable"
    , doc = FormParams.toJson(Text(
      name = "expectedTable"
      , value = ""
      , extra = Extra(
        doc = "The expectedTable name for PSI calculation"
        , label = "expectedTable"
        , options = Map(
        )
      )
    )
    )
  )

  final val binningTable: Param[String] = new Param[String](parent = this
    , name = "binningTable"
    , doc = FormParams.toJson(Text(
      name = "binningTable"
      , value = ""
      , extra = Extra(
        doc = "The binningTable name for PSI calculation"
        , label = "binningTable"
        , options = Map(
        )
      )
    )
    )
  )

  final val binningPath: Param[String] = new Param[String](parent = this
    , name = "binningPath"
    , doc = FormParams.toJson(Text(
      name = "binningPath"
      , value = ""
      , extra = Extra(
        doc = "The binningPath that store the binning result"
        , label = "binningPath"
        , options = Map(
        )
      )
    )
    )
  )

  final val actualTable: Param[String] = new Param[String](parent = this
    , name = "actualTable"
    , doc = FormParams.toJson(Text(
      name = "actualTable"
      , value = ""
      , extra = Extra(
        doc = "The actualTable name for PSI calculation"
        , label = "actualTable"
        , options = Map(
        )
      )
    )
    )
  )

  final val selectedFeatures: Param[String] = new Param[String](parent = this
    , name = "selectedFeatures"
    , doc = FormParams.toJson(Text(
      name = "selectedFeatures"
      , value = ""
      , extra = Extra(
        doc = "The features (column name) that involved in Binning and PSI process."
        , label = "selectedFeatures"
        , options = Map(
        )
      )
    )
    )
  )


  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("load is not supported in PSI ET")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("predict is not supported in PSI ET")
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_PSI_operator__"),
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
  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |run train_data as PSI.`` where
      |expectedTable='train_data' and
      |actualTable='test_data' and
      |binningTable='binningInfoTable' and
      |binningPath='/tmp/**/binning' and
      |selectedFeatures='LIMIT_BAL,SEX,EDUCATION,MARRIAGE,AGE,PAY_0,PAY_2' as res1;
      |
      |;
    """.stripMargin)
}

object PSIObject {
  val BINNING_EXT = "Binning";

  def BINNING_RESULT_COLUNM_NAME_CONVERT(originColName: String): String = {
    originColName + "_output"
  }
}

