package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.ml.feature.{DiscretizerFeature, Normalizer, OneHotEncoder, OneHotEncoderModel, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.MetaConst.getMetaPath
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/2/10 15:07
 *
 */
case class OnehotExt(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {

  def this() = this(BaseParams.randomUID())

  def get_dataType(selectedFeatures: Array[String], df: DataFrame): Array[String] = {
    val dTypeCols = df.select(selectedFeatures.head, selectedFeatures.toList.tail: _*).limit(1).dtypes.map(
      m => {
        m._2 match {
          case "StringType" => "String"
          case "DoubleType" => "Double"
          case "IntegerType" => "Integer"
          case "LongType" => "Long"
          case _ => throw new RuntimeException(s"Unsupported Feature Type in ${m._1}")
        }
      }
    )
    dTypeCols
  }

  def onehotFunc(df: DataFrame, path: String, featureCols: Array[String]): DataFrame = {

    val metaPath = getMetaPath(path)

    val colTypeArray = get_dataType(featureCols, df)

    val outputIndexCols = featureCols.map(col => {
      col + "_index"
    })

    val outputOnehotCols = featureCols.map(col => {
      col + "_output"
    })

    val indexer = new StringIndexer()
      .setInputCols(featureCols)
      .setOutputCols(outputIndexCols)
      .fit(df)
    val siPath = s"$metaPath/columns/stringindexer/"
    indexer.asInstanceOf[MLWritable].write.overwrite().save(siPath)

    val indexedDF = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCols(outputIndexCols)
      .setOutputCols(outputOnehotCols).setDropLast(false).fit(indexedDF)
    val ecPath = s"$metaPath/columns/encoder"
    val encodedDF = encoder.transform(indexedDF)
    encoder.asInstanceOf[MLWritable].write.overwrite().save(ecPath)

    val meta = Array((0, OneHotTrainingData(siPath, ecPath, colTypeArray)))
    val spark = df.sparkSession
    import spark.implicits._
    spark.createDataset(meta).write.mode(SaveMode.Overwrite).parquet(OneHotModelTool.ONEHOTMODEL_PATH(metaPath))
    encodedDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val featureCols = params.getOrElse(selectedFeatures.name, "").split(",")
    require(!featureCols.isEmpty, "The selected features has to be setup!")
    onehotFunc(df, path, featureCols)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    import sparkSession.implicits._
    val metapath = getMetaPath(path)
    val metas = sparkSession.read
      .parquet(OneHotModelTool.ONEHOTMODEL_PATH(metapath))
      .as[(Int, OneHotTrainingData)]
      .collect()
      .sortBy(_._1)
      .map(_._2)
    val siPath = metas(0).stringIndexerPath
    val omPath = metas(0).onehotModelPath
    val stringIndexer = StringIndexerModel.load(siPath)
    val onehotEncoder = OneHotEncoderModel.load(omPath)
    val dtypeArray = metas(0).dTypeArray
    (stringIndexer, onehotEncoder, dtypeArray)
  }

  final val selectedFeatures: Param[String] = new Param[String](parent = this
    , name = "selectedFeatures"
    , doc = FormParams.toJson(Text(
      name = "selectedFeatures"
      , value = ""
      , extra = Extra(
        doc = "The selected columns that plan to be involved in the OneHot process"
        , label = DataTranspose.INPUT_COL
        , options = Map(
        )
      )
    )
    )
  )

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |set jsonStr='''
      |{"subject":"数学","name":"张三","score":88},
      |{"subject":"语文","name":"张三","score":92}
      |{"subject":"英语","name":"张三","score":77}
      |{"subject":"数学","name":"王五","score":65}
      |{"subject":"语文","name":"王五","score":87}
      |{"subject":"英语","name":"王五","score":90}
      |{"subject":"数学","name":"李雷","score":67}
      |{"subject":"语文","name":"李雷","score":33}
      |{"subject":"英语","name":"李雷","score":24}
      |{"subject":"数学","name":"宫九","score":77}
      |{"subject":"语文","name":"宫九","score":87}
      |{"subject":"英语","name":"宫九","score":90}
      |''';
      |
      |load jsonStr.`jsonStr` as data;
      |run data as Onehot.`/tmp/onehottest`
      |where selectedFeatures='name'
      |as onehotTable;
      |;
    """.stripMargin)

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not supported in OnehotExt")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val (stringIndexer, onehotModel, dTypeArray) = load(df.sparkSession, path, params).asInstanceOf[(StringIndexerModel, OneHotEncoderModel, Array[String])]
    val df_after_idx = stringIndexer.transform(df)
    val new_res = onehotModel.transform(df_after_idx)
    new_res
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_onehot_operator__"),
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

case class OneHotTrainingData(
                               stringIndexerPath: String,
                               onehotModelPath: String,
                               dTypeArray: Array[String]
                             )

object OneHotModelTool {
  def ONEHOTMODEL_PATH(path: String) = s"$path/columns/one"
}