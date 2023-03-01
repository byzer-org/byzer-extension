package tech.mlsql.plugins.connectpersist

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ConnectMeta
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.store.DBStore
import tech.mlsql.version.VersionCompatibility

/**
 * 15/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ConnectPersistCommand(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    val dbMapping = ConnectMeta.toMap
    val items = dbMapping.toList.map(f => ConnectMetaItem(f._1.format, f._1.db, f._2))
    import session.implicits._
    val newdf = session.createDataset[ConnectMetaItem](items).toDF()
    DBStore.store.saveTable(session, newdf, ConnectPersistMeta.connectTableName, Option("format,db"), false)
    newdf
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |
       |```
       |${codeExample.code}
       |```
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |example
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

}

object ConnectPersistMeta {
  def connectTableName = "__mlsql__.connect_table"
}

case class ConnectMetaItem(format: String, db: String, options: Map[String, String])
