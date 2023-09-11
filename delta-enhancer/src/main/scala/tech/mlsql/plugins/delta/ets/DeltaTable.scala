package tech.mlsql.plugins.delta.ets

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.datalake.DataLake
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.delta.app.ByzerDelta
import tech.mlsql.tool.HDFSOperatorV2
import tech.mlsql.version.VersionCompatibility

class DeltaTable(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    import df.sparkSession.implicits._
    val spark = df.sparkSession

    def resolveRealPath(dataPath: String) = {
      val dataLake = new DataLake(spark)
      if (dataLake.isEnable) {
        dataLake.identifyToPath(dataPath)
      } else {
        PathFun(path).add(dataPath).toPath
      }
    }

    val args = JSONTool.parseJson[List[String]](params("parameters"))

    args match {
      case Seq("desc", dataPath) =>
        val deltaLog = DeltaTable.forPath(spark, resolveRealPath(dataPath))
        // 查看表的schema信息
        val schema = deltaLog.toDF.schema.map { f =>
          (f.name, f.dataType.typeName, f.metadata.toString())
        }.toDF("name", "type", "meta")
        schema
      case Seq("truncate", dataPath) =>
        // 虚假的删除，只是修改了metadata信息
        val deltaLog = DeltaTable.forPath(spark, resolveRealPath(dataPath))
        // 删除表
        deltaLog.delete()
        spark.emptyDataFrame
      case Seq("delete", dataPath, condition) =>
        val deltaLog = DeltaTable.forPath(spark, resolveRealPath(dataPath))
        // 根据条件删除数据
        deltaLog.delete(condition)
        spark.emptyDataFrame
      case Seq("vacuum", dataPath, howManyHoures) =>
        val deltaLog = DeltaTable.forPath(spark, resolveRealPath(dataPath))
        deltaLog.vacuum(howManyHoures.toDouble)
        spark.emptyDataFrame
      case Seq("remove", dataPath) =>
        val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
        fs.delete(new Path(resolveRealPath(dataPath)), true)
        spark.emptyDataFrame
      case _ =>
        throw new MLSQLException(s"not support command ${args.mkString(" ")}")
    }
  }

  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = ByzerDelta.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }
}