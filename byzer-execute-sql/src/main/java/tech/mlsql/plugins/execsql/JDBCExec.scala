package tech.mlsql.plugins.execsql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool

import tech.mlsql.version.VersionCompatibility

class JDBCExec(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    val session = ScriptSQLExec.context().execListener.sparkSession

    // !exec_sql ''' select * from table ''' by connName;
    // !exec_sql tableName from ''' select * from table ''' by connName;
    args match {
      case List(sql, "by", connName) =>
        JobUtils.executeQueryInDriverWithoutResult(session, connName, sql)

      case List( tableName, "from", sql, "by", connName) =>
        val newDF = JobUtils.executeQueryWithDiskCacheParquet(session, connName, sql)
        newDF.createOrReplaceTempView(tableName)
      case _ =>
        throw new RuntimeException("!exec_sql connName ''' select * from xxxx ''';")
    }
    session.emptyDataFrame
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = {
    Seq(">=1.6.0")
  }

}
