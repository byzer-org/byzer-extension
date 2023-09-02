package tech.mlsql.plugins.assert.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.assert.app.MLSQLAssert
import tech.mlsql.version.VersionCompatibility

class AssertNotNull(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    println(df, path, params)
    var args = JSONTool.parseJson[List[String]](params("parameters"))

    args match {
      case List(tableName, columns) =>
        val session = df.sparkSession
        val validateData = session.table(tableName)
        // 根据columns查找对应的fieldIndex
        val fieldIndexs = columns.split(",").map(validateData.schema.fieldIndex(_))
        // 选出这几列中有null的数据
        val nullData = validateData.filter(row => {
          fieldIndexs.find(row.isNullAt(_)).isDefined
        })
        nullData
      case _ =>
        throw new MLSQLException("AssertNotNull only support !assertNotNull {table} '{columns}'")
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
