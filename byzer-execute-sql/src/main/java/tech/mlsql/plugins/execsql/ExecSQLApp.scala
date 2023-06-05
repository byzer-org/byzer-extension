package tech.mlsql.plugins.execsql

import net.sf.json.JSONObject
import org.apache.spark.sql.SparkSession
import streaming.core.datasource.JDBCUtils
import streaming.core.datasource.JDBCUtils.formatOptions
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

import java.util.concurrent.ConcurrentHashMap

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ExecSQLApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {

    ETRegister.register("JDBCConn", classOf[JDBCConn].getName)
    CommandCollection.refreshCommandMapping(Map("conn" ->
      """
        |run command as JDBCConn.`` where parameters='''{:all}'''
        |""".stripMargin))

    ETRegister.register("JDBCExec", classOf[JDBCExec].getName)
    CommandCollection.refreshCommandMapping(Map("exec_sql" ->
      """
        |run command as JDBCExec.`` where parameters='''{:all}'''
        |""".stripMargin))
  }

  override def supportedVersions: Seq[String] = {
    ExecSQLApp.versions
  }

}

object ExecSQLApp {
  val versions = Seq(">=2.0.1")

  private val connectionPool = new ConcurrentHashMap[String,java.sql.Connection]()


  def executeQueryInDriverWithoutResult(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val stat = ExecSQLApp.connectionPool.get(connName).prepareStatement(sql)
    stat.execute()
  }


  def executeQueryInDriver(session:SparkSession, connName:String, sql:String) = {
    import scala.collection.JavaConverters._
    val stat = ExecSQLApp.connectionPool.get(connName).prepareStatement(sql)
    val rs = stat.executeQuery()
    val res = JDBCUtils.rsToMaps(rs)
    stat.close()
    val rdd = session.sparkContext.parallelize(res.map(item => JSONObject.fromObject(item.asJava).toString()))
    session.read.json(rdd)
  }

  def newConnection(name: String, options: Map[String, String]) = synchronized {
    val driver = options("driver")
    val url = options("url")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, formatOptions(options))
    if (ExecSQLApp.connectionPool.containsKey(name)) {
      ExecSQLApp.connectionPool.get(name).close()
    }
    ExecSQLApp.connectionPool.put(name, connection)
    ExecSQLApp.connectionPool.get(name)
  }

  def removeConnection(name: String) = synchronized {
    if (ExecSQLApp.connectionPool.containsKey(name)) {
      val connection = ExecSQLApp.connectionPool.get(name)
      connection.close()
      ExecSQLApp.connectionPool.remove(name)
    }
  }
}
