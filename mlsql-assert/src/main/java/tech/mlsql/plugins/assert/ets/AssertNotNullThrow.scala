package tech.mlsql.plugins.assert.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.assert.app.MLSQLAssert
import tech.mlsql.version.VersionCompatibility

class AssertNotNullThrow(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    println(df, path, params)
    var args = JSONTool.parseJson[List[String]](params("parameters"))

    args match {
      case List(tableName, columns) =>
        val session = df.sparkSession
        // 从spark中选出这几列
        val validateData = session.sql(s"select ${columns} from ${tableName}")
        // 选出这几列中有null的数据
        val nullData = validateData.filter(row => row.anyNull)
        // 如果有null的数据，抛出异常
        val dirtyCount = nullData.count()
        if (dirtyCount > 0) {
          throw new MLSQLException(s"Assert Failed: '$tableName' dirty count: ${dirtyCount}")
        }
      case _ =>
        throw new MLSQLException("AssertNotNullThrow only support !assertNotNullThrow {table} '{columns}'")
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
