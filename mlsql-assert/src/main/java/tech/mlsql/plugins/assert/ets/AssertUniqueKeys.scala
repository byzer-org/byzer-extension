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

class AssertUniqueKeys(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    println(df, path, params)
    var args = JSONTool.parseJson[List[String]](params("parameters"))

    args match {
      case List(tableName, columns) =>
        val session = df.sparkSession
        val fieldNames = session.table(tableName).schema.fieldNames
        // fieldNames通过,号连接
        val sourceColumns = fieldNames.mkString(",")
        session.sql(
          s"""
             |select ${sourceColumns} from (
             |select ${sourceColumns}, count(*) over(partition by ${columns}) as cnt from ${tableName}
             |)a
             |where a.cnt > 1
             |""".stripMargin)
      case _ =>
        throw new MLSQLException("AssertUniqueKeys only support !assertUniqueKeys {table} '{columns}'")
    }
  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = MLSQLAssert.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }
}

