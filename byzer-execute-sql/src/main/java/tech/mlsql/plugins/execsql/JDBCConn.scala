package tech.mlsql.plugins.execsql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool

import tech.mlsql.version.VersionCompatibility

class JDBCConn(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    import df.sparkSession.implicits._

    // connect to greenplm as gpconn(dsn=clddb authdomain=sjgl_lauth);
    // !conn gpconn "url=jdbc:postgresql://xxxx" "username=xxxx" "password=xxxx" "driver=org.postgresql.Driver";
    // !conn remove gpconn;
    args match {
      case List("remove", connName) =>
        JobUtils.removeConnection(connName)

      case connName :: left =>
        val options = left.map { item =>
          val Array(key, value) = item.split("=", 2)
          (key, value)
        }.toMap
        JobUtils.newConnection(connName, options)
    }

    df.sparkSession.emptyDataFrame
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
