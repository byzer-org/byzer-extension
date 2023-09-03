package tech.mlsql.plugins.assert.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.assert.app.MLSQLAssert
import tech.mlsql.version.VersionCompatibility

class AssertUniqueKeyThrow(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    println(df, path, params)
    var args = JSONTool.parseJson[List[String]](params("parameters"))

    args match {
      case List(tableName, columns) =>
        val session = df.sparkSession
        val validateData = session.sql(s"select ${columns} from ${tableName}")
        // 分别校验每一列distinct之后的数据量和count对比
        val validateResult = columns.split(",").filter(column => {
          // 获得对应列的数据量，并且过滤掉所有null值
          val validateCount = validateData.select(column).filter(row => !row.anyNull).count()
          val distinctCount = validateData.select(column).filter(row => !row.anyNull).distinct().count()
          distinctCount != validateCount
        })
        if (validateResult.length > 0) {
          throw new MLSQLException(s"Assert Failed: conflict key: ${validateResult.mkString(",")}")
        }
      case _ =>
        throw new MLSQLException("AssertUniqueKeyThrow only support !assertUniqueKeyThrow {table} '{columns}'")
    }
    df.sparkSession.emptyDataFrame
  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = MLSQLAssert.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }
}

