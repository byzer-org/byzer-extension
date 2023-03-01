package tech.mlsql.plugins.mllib.ets.fintech.scorecard

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.feature.{Bucketizer, DiscretizerFeature, QuantileDiscretizer, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.param.Param
import tech.mlsql.common.utils.log.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, MLSQLUtils, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, udf, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{Code, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.MetaConst.{DISCRETIZER_PATH, getMetaPath}
import streaming.dsl.mmlib.algs.meta.DiscretizerMeta
import streaming.dsl.mmlib.algs.{CodeExampleText, DiscretizerTrainData, Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.{ETMethod, PREDICT}
import org.apache.spark.sql.functions._
import org.apache.spark.util.collection.OpenHashMap
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}
import tech.mlsql.dsl.adaptor.MLMapping

import scala.collection.mutable
import scala.util.parsing.json.{JSON, JSONObject}

/**
 *
 * @Author; Andie Huang
 * @Date: 2021/12/28 14:25
 *
 */
class SQLBinning(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth with Logging {

  def this() = this(BaseParams.randomUID())

  def getBinningInfoByFeatureName(featureName: String, table: DataFrame, labelCol: String, goodValue: Int, featuresMapToSplits: Map[String, Array[Double]]): Map[String, Any] = {
    val GOOD_VALUE = goodValue
    val SAMPLE_COUNT = table.count()
    val POSITIVE_COUNT = table.select(labelCol).where(col(labelCol) === GOOD_VALUE).count().toDouble
    val NEGATIVE_COUNT = (SAMPLE_COUNT - POSITIVE_COUNT).toDouble
    require(NEGATIVE_COUNT > 0 && POSITIVE_COUNT > 0, "The categories of the samples are unbalanced!")
    val aggDF = table.select(Array(labelCol, featureName + "_output").map(col(_)): _*).groupBy(featureName + "_output", labelCol).count().orderBy(featureName + "_output")
    aggDF.show()
    val splits = featuresMapToSplits.getOrElse(featureName, Array())
    val splitLen = splits.length
    var biningMap: Map[String, Any] = Map()
    var intervalInfo = List[Any]()
    var totalIV = 0.0
    for (i <- 0 to splitLen - 2) {
      val pRow = aggDF.select("count").where(aggDF(featureName + "_output") === i && aggDF(labelCol) === GOOD_VALUE).collect()
      val p = pRow.isEmpty match {
        case false => pRow(0).getLong(0).toDouble
        case true => 0.toDouble
      }
      println("p:" + p)
      val nRow = aggDF.select("count").where(aggDF(featureName + "_output") === i && aggDF(labelCol) === 1 - GOOD_VALUE).collect()
      val n = nRow.isEmpty match {
        case false => nRow(0).getLong(0).toDouble
        case true => 0.toDouble
      }
      println("n:" + n)
      val total = n + p
      println("total:" + total)
      val prate = total match {
        case 0.0 => 0.0
        case _ => p / total
      }
      println("prate:" + prate)
      val woe = Math.log(prate / (POSITIVE_COUNT / NEGATIVE_COUNT))
      println("woe:" + woe)
      val iv = (p / POSITIVE_COUNT - n / NEGATIVE_COUNT) * woe
      totalIV += iv
      println("iv:" + iv)
      val start = splits(i)
      val end = splits(i + 1)
      val value = i match {
        case 0 => s"($start, $end)"
        case _ => s"[$start, $end)"
      }
      println(s"value: $value")
      intervalInfo = intervalInfo :+ Map("p" -> p, "n" -> n, "prate" -> prate, "total" -> total, "woe" -> woe, "iv" -> iv, "value" -> value, "bin_id" -> i)
    }
    biningMap += ("bin" -> intervalInfo, "iv" -> totalIV)
    biningMap
  }

  def getBinningInfoByStrFeatureName(featureName: String, table: DataFrame, labelCol: String, goodValue: Int, featuresMapToLabelArray: Map[String, Array[String]]): Map[String, Any] = {
    val GOOD_VALUE = goodValue
    val SAMPLE_COUNT = table.count()
    val POSITIVE_COUNT = table.select(labelCol).where(col(labelCol) === GOOD_VALUE).count().toDouble
    val NEGATIVE_COUNT = (SAMPLE_COUNT - POSITIVE_COUNT).toDouble
    require(NEGATIVE_COUNT > 0 && POSITIVE_COUNT > 0, "The categories of the samples are unbalanced!")
    val aggDF = table.select(Array(labelCol, featureName + "_output").map(col(_)): _*).groupBy(featureName + "_output", labelCol).count().orderBy(featureName + "_output")
    aggDF.show()
    val labelArray = featuresMapToLabelArray.getOrElse(featureName, Array())
    var biningMap: Map[String, Any] = Map()
    var totalIV = 0.0
    var intervalInfo = List[Any]()
    for (i <- 0 to labelArray.length - 1) {
      val pRow = aggDF.select("count").where(aggDF(featureName + "_output") === i && aggDF(labelCol) === GOOD_VALUE).collect()
      val p = pRow.isEmpty match {
        case false => pRow(0).getLong(0).toDouble
        case true => 0.toDouble
      }
      println("p:" + p)
      val nRow = aggDF.select("count").where(aggDF(featureName + "_output") === i && aggDF(labelCol) === 1 - GOOD_VALUE).collect()
      val n = nRow.isEmpty match {
        case false => nRow(0).getLong(0).toDouble
        case true => 0.toDouble
      }
      println("n:" + n)
      val total = n + p
      println("total:" + total)
      val prate = total match {
        case 0.0 => 0.0
        case _ => p / total
      }
      println("prate:" + prate)
      val woe = Math.log(prate / (POSITIVE_COUNT / NEGATIVE_COUNT))
      println("woe:" + woe)
      val iv = (p / POSITIVE_COUNT - n / NEGATIVE_COUNT) * woe
      totalIV += iv
      println("iv:" + iv)
      val label = labelArray(i)
      val value = s"[$label]"
      println(s"value: $value")
      intervalInfo = intervalInfo :+ Map("p" -> p, "n" -> n, "prate" -> prate, "total" -> total, "woe" -> woe, "iv" -> iv, "value" -> value, "bin_id" -> i)
    }
    biningMap += ("bin" -> intervalInfo, "iv" -> totalIV)
    biningMap
  }

  def getConfigMapFromModel(discretizerModel: Bucketizer): Map[String, Array[Double]] = {
    var featuresMapToSplits = Map[String, Array[Double]]()
    val inputs = discretizerModel.getInputCols
    for (i <- 0 to inputs.length - 1) {
      val fea = inputs(i)
      val splits = discretizerModel.getSplitsArray(i)
      featuresMapToSplits += (fea -> splits)
    }
    featuresMapToSplits
  }

  def getLabelArrayByIndexer(indexer: StringIndexerModel): Map[String, Array[String]] = {
    var featuresMapToLabelArray = Map[String, Array[String]]()
    val inputs = indexer.getInputCols
    for (i <- 0 to inputs.length - 1) {
      val fea = inputs(i)
      val splits = indexer.labelsArray(i)
      featuresMapToLabelArray += (fea -> splits)
    }
    featuresMapToLabelArray
  }

  def EFBiningTraining(featureBucketNumMap: Map[String, Int], df: DataFrame): (DataFrame, Bucketizer) = {
    val fi = featureBucketNumMap.toArray
    val inputs: Array[String] = fi.map(_._1)
    val bucketNumArray: Array[Int] = fi.map(_._2)
    val discretizer = new QuantileDiscretizer().setNumBucketsArray(bucketNumArray)
      .setInputCols(inputs)
      .setOutputCols(inputs.map(inCol => inCol + "_output"))
      .setRelativeError(0.01)
    val discretizerModel = discretizer.fit(df)
    val transformedDF = discretizerModel.transform(df)
    (transformedDF, discretizerModel)
  }

  def EDBinningTraining(featureSplitsMap: Map[String, Array[Double]], df: DataFrame): (DataFrame, Bucketizer) = {
    val fi = featureSplitsMap.toArray
    val inputs: Array[String] = fi.map(_._1)
    val splitsArray: Array[Array[Double]] = fi.map(_._2)
    val bucketizer = new Bucketizer()
      .setInputCols(inputs)
      .setSplitsArray(splitsArray)
      .setOutputCols(inputs.map(inCol => inCol + "_output"))
    val transformedDF = bucketizer.transform(df)
    (transformedDF, bucketizer)
  }

  def getBucketNumMap(params: Map[String, String], processFeatures: Array[String]): Map[String, Int] = {
    // val processFeatures = "col1,col2,col3,col4";
    //    val processFeatures = params.getOrElse(Binning.SELECTED_FEATURES, "")
    //    require(!processFeatures.isEmpty, "The param [processFeatures] is required!")
    val numBucketStr = params.getOrElse(numBucketParam.name, "")
    require(!numBucketStr.isEmpty, "The param [numBucket] is required!")
    val numBucket = numBucketStr.toInt
    val bucketMap = processFeatures.map(t => {
      (t, numBucket)
    }).toMap
    var res = Map() ++ bucketMap
    //    val customizedBuckets = "col1:10,col2:100"
    val customizedBuckets = params.getOrElse(customizedConfigParam.name, "")
    if (!customizedBuckets.isEmpty) {
      customizedBuckets.split(",").map(f => {
        val meta = f.split(":")
        val key = meta(0)
        val value = meta(1).toInt
        res += (key -> value)
      })
    }
    res
  }

  def parseParamsForED(params: Map[String, String], df: DataFrame, processFeatures: Array[String]): Map[String, Array[Double]] = {
    val bucketNumMap = getBucketNumMap(params, processFeatures)
    var splitsArrayMap: Map[String, Array[Double]] = Map()
    bucketNumMap.map(c => {
      val colName = c._1
      val numBucket = c._2
      val summary = df.describe(colName)
      val max = summary.filter("summary = 'max'").select(col(colName)).collect().map(_ (0)).toList(0).toString.toDouble
      val min = summary.filter("summary = 'min'").select(col(colName)).collect().map(_ (0)).toList(0).toString.toDouble
      val splits = getSplits(numBucket, max, min)
      splitsArrayMap += (colName -> splits)
    })
    splitsArrayMap
  }

  def getSplits(numBuckets: Int, max_value: Double, min_value: Double): Array[Double] = {
    val step = (max_value - min_value) / numBuckets
    val valueWindow = new Array[Double](numBuckets + 1)
    valueWindow(0) = Double.NegativeInfinity
    for (i <- 1 until (numBuckets - 2)) {
      valueWindow(i) = min_value + step * i
    }
    valueWindow(numBuckets - 1) = Double.PositiveInfinity // 共 N - 1 个桶
    valueWindow
  }


  def findOutStrCols(df: DataFrame, params: Map[String, String]): Array[String] = {
    df.columns
      .map(c => {
        df.select(c).limit(1).dtypes(0)
      }).filter(_._2 == StringType.toString)
      .map(_._1)
  }

  def seperatedSelectedFeaByType(df: DataFrame, selectedFeatures: Array[String]): (Array[String], Array[String]) = {
    val selectedDF = df.select(selectedFeatures.head, selectedFeatures.tail: _*)
    val stringCols = selectedDF.columns.map(c => {
      selectedDF.select(c).limit(1).dtypes(0)
    }).filter(_._2 == StringType.toString)
      .map(_._1)
    val otherCols = selectedDF.columns.filter(c => {
      !stringCols.contains(c)
    })
    (stringCols, otherCols)
  }

  def binningForStringCols(df: DataFrame, strCols: Array[String]): (DataFrame, StringIndexerModel) = {
    if (strCols.length == 0) {
      return (df, null)
    }
    val indexer = new StringIndexer()
      .setInputCols(strCols)
      .setOutputCols(strCols.map(_ + "_output"))
      .fit(df)
    val transformedDF = indexer.transform(df)
    (transformedDF, indexer)
  }

  def getStringIndexMapFromModel(indexer: StringIndexerModel): Map[String, Array[String]] = {
    indexer.getInputCols.zip(indexer.labelsArray).map(m => {
      (m._1, m._2)
    }).toMap
  }

  def getLabelIndexArray(indexer: StringIndexerModel): (Array[String], Array[Array[String]]) = {
    (indexer.getInputCols, indexer.labelsArray)
  }

  def getIndexer(labelToIndex: mutable.HashMap[String, Double]) = {

    udf { label: String =>
      if (label == null) {
        throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
          "NULLS, try setting StringIndexer.handleInvalid.")

      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else {
          throw new SparkException(s"Unseen label: $label. To handle unseen labels, ")
        }
      }
    }.asNondeterministic()
  }

  def transformFromLabelsArray(df: DataFrame, inputCols: Array[String], labelsArray: Array[Array[String]]): DataFrame = {
    var processedDF = df
    val labelsToIndexArray: Array[mutable.HashMap[String, Double]] = {
      for (labels <- labelsArray) yield {
        val n = labels.length
        val map = new mutable.HashMap[String, Double]
        labels.zipWithIndex.foreach { case (label, idx) =>
          map.update(label, idx)
        }
        map
      }
    }
    val outputColumns = new Array[Column](inputCols.length)
    for (i <- 0 until inputCols.length) {
      val inputColName = inputCols(i)
      val outputColName = inputColName + "_output"
      val labelToIndex = labelsToIndexArray(i)
      val labels = labelsArray(i)
      if (!df.schema.fieldNames.contains(inputColName)) {
        print(s"Input column ${inputColName} does not exist during transformation. " +
          "Skip StringIndexerModel for this column.")
      }
      val metadata = NominalAttribute.defaultAttr
        .withName(outputColName)
        .withValues(labels)
        .toMetadata()
      val indexer = getIndexer(labelToIndex)
      outputColumns(i) = indexer(df(inputColName).cast(StringType))
        .as(outputColName, metadata)
      processedDF = processedDF.withColumn(outputColName, outputColumns(i))
    }
    processedDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._


    // val processFeatures = "col1,col2,col3,col4";
    val selectedFetauresStr = params.getOrElse(selectedFeaturesParam.name, "")
    require(!selectedFetauresStr.isEmpty, "The param [processFeatures] is required!")
    val goodValue = params.getOrElse(goodValueParam.name, "1").toInt
    val selectedFetaures = selectedFetauresStr.split(",")
    val dTypeCols = df.select(selectedFetaures.head, selectedFetaures.toList.tail: _*).limit(1).dtypes.map(
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
    // firstly check if there exist str cols
    val (strCols, otherCols) = seperatedSelectedFeaByType(df, selectedFetaures)
    val (transformedDF, stringIndexr) = binningForStringCols(df, strCols)
    val labelsArray = stringIndexr match {
      case indexr: StringIndexerModel => indexr.labelsArray
      case _ => Array.ofDim[String](0, 0)
    }

    // df.select("label").dtypes(0)._2 == DoubleType.toString
    val method = params.getOrElse(binningMethodParam.name, "EF")
    val labelCol = params.getOrElse(labelColNameParam.name, "label")
    //    val goodValue = params.getOrElse("")
    val processFeatures = otherCols
    val (res, model) = method match {
      case "EF" =>
        val parsedParams = getBucketNumMap(params, processFeatures)
        EFBiningTraining(parsedParams, transformedDF)
      case "ED" =>
        val splitsArray = parseParamsForED(params, df, processFeatures)
        EDBinningTraining(splitsArray, transformedDF)
    }


    // Store the model
    val binningPath = Binning.generateBinningPath(path)
    val metaPath = getMetaPath(binningPath)
    val metas = Array((0, BinningTrainData(model.getInputCols, model.getOutputCols, false, model.getSplitsArray, strCols, labelsArray, dTypeCols)))
    spark.createDataset(metas).write.mode(SaveMode.Overwrite).parquet(DISCRETIZER_PATH(metaPath))

    val binMethod = method match {
      case "EF" => "quantile"
      case "ED" => "bucket"
    }
    val binningInfoData = otherCols.map(fea => {
      val fea2Splits = getConfigMapFromModel(model)
      val binningInfo = getBinningInfoByFeatureName(fea, res, labelCol, goodValue, fea2Splits)
      (fea, JSONTool.toJsonStr(binningInfo))
    }).toSeq ++ strCols.map(fea => {
      val fea2LabelArray = getLabelArrayByIndexer(stringIndexr)
      val binningInfo = getBinningInfoByStrFeatureName(fea, res, labelCol, goodValue, fea2LabelArray)
      (fea, JSONTool.toJsonStr(binningInfo))
    })


    val binningInfoColumns = Seq("featureName", "Binning_Info")
    val binningInfoTable = spark.createDataFrame(binningInfoData).toDF(binningInfoColumns: _*)
    binningInfoTable
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    val binningPath = Binning.generateBinningPath(_path)
    val path = getMetaPath(binningPath)
    val metas = spark.read
      .parquet(DISCRETIZER_PATH(path))
      .as[(Int, BinningTrainData)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val metasbc = spark.sparkContext.broadcast(metas)
    val loadedModel = metasbc.value(0)
    val labelArrays = loadedModel.stringLabelsArray
    val stringCols = loadedModel.stringInputs
    val dtypeCols = loadedModel.dTypeCols
    val bucketizer = new Bucketizer()
      .setInputCols(loadedModel.inputCols)
      .setOutputCols(loadedModel.outputsCols)
      .setSplitsArray(loadedModel.splitsArray)
    (stringCols, labelArrays, bucketizer, dtypeCols)
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
      |;
    """.stripMargin)

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    //    val meta = _model.asInstanceOf[Bucketizer]
    //    val transformer: Seq[Double] => Seq[Double] = features => {
    //      features.zipWithIndex.map {
    //        case (feature, index) =>
    //          val splits = meta.getSplitsArray(index)
    //          DiscretizerFeature.binarySearchForBuckets(splits, feature, false)
    //      }
    //    }

    val meta = _model.asInstanceOf[(Array[String], Array[Array[String]], Bucketizer, Array[String])]
    val dtypeCols = meta._4
    val bucketizer = meta._3
    val labelArrays = meta._2
    val transformer: Seq[String] => Seq[Double] = features => {
      var strIdx = 0
      var otherIdx = 0
      features.zipWithIndex.map {
        case (feature, index) => {
          dtypeCols(index) match {
            case "String" =>
              val value = feature
              val labelArray = labelArrays(strIdx)
              strIdx += 1
              DiscretizerFeature.strColIndexSearch(labelArray, value)
            case "Double" | "Integer" | "Long" =>
              val value = feature.toDouble
              val split = bucketizer.getSplitsArray(otherIdx)
              otherIdx += 1
              DiscretizerFeature.binarySearchForBuckets(split, value, false)
          }
        }
      }
    }

    MLSQLUtils.createUserDefinedFunction(transformer, ArrayType(DoubleType), Some(Seq(ArrayType(DoubleType))))
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val (stringCols, labelArrays, bucketizer, dtypeCols) = load(df.sparkSession, path, params).asInstanceOf[(Array[String], Array[Array[String]], Bucketizer, Array[String])]
    var transformedDF = df
    if (stringCols.length != 0) {
      transformedDF = transformFromLabelsArray(df, stringCols, labelArrays)
    }
    bucketizer.transform(transformedDF)
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_binning_operator__"),
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

  val labelColNameParam: Param[String] = new Param[String](this, Binning.LABEL_COLNAME,
    FormParams.toJson(Text(
      name = Binning.LABEL_COLNAME,
      value = "",
      extra = Extra(
        doc =
          """
            | The column name of the label
          """,
        label = "The column name of the label",
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

  val numBucketParam: Param[String] = new Param[String](this, Binning.NUM_BUCKETS,
    FormParams.toJson(Text(
      name = Binning.NUM_BUCKETS,
      value = "",
      extra = Extra(
        doc =
          """
            | The number of bucket that split the features
          """,
        label = "The number of bucket",
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

  val customizedConfigParam: Param[String] = new Param[String](this, Binning.CUSTOMIZED_CONFIG,
    FormParams.toJson(Text(
      name = Binning.CUSTOMIZED_CONFIG,
      value = "",
      extra = Extra(
        doc =
          """
            | The customized config setting for each column. e.g., col1:3,col2:4 means the column col1
            |  will be binned with bukcet number 3 and the col2 will be binned with 4 buckets.
          """,
        label = "The customized config setting for each column!",
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

  val goodValueParam: Param[String] = new Param[String](this, Binning.GOOD_VALUE,
    FormParams.toJson(Text(
      name = Binning.GOOD_VALUE,
      value = "",
      extra = Extra(
        doc =
          """
            | The number that indicates the good users, which is default 1
          """,
        label = "The customized config setting for each column!",
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

  val binningMethodParam: Param[String] = new Param[String](this, Binning.BINNING_METHOD,
    FormParams.toJson(
      Select(
        name = Binning.BINNING_METHOD,
        values = List(),
        extra = Extra(
          doc = "",
          label = "",
          options = Map(
            "valueType" -> "string",
            "required" -> "true",
          )), valueProvider = Option(() => {
          List(
            KV(Some(Binning.BINNING_METHOD), Some(Binning.EF_BINNING_METHOD)),
            KV(Some(Binning.BINNING_METHOD), Some(Binning.ED_BINNING_METHOD))
          )
        })
      )
    )
  )

}

object Binning {
  val SELECTED_FEATURES = "selectedFeatures"
  val NUM_BUCKETS = "numBucket"
  val CUSTOMIZED_CONFIG = "customizedBucketsConfig"
  val BINNING_METHOD = "method"
  val LABEL_COLNAME = "label"
  val GOOD_VALUE = "goodValue"
  val EF_BINNING_METHOD = "EF"
  val ED_BINNING_METHOD = "ED"

  def generateBinningPath(path: String): String = {
    s"${path.stripSuffix("/")}/binning"
  }
}


case class BinningTrainData(inputCols: Array[String],
                            outputsCols: Array[String],
                            handleInvalid: Boolean,
                            splitsArray: Array[Array[Double]],
                            stringInputs: Array[String],
                            stringLabelsArray: Array[Array[String]],
                            dTypeCols: Array[String])
