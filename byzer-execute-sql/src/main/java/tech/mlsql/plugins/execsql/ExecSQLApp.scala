package tech.mlsql.plugins.execsql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import streaming.core.datasource.JDBCUtils
import streaming.core.datasource.JDBCUtils.formatOptions
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.tool.HDFSOperatorV2
import tech.mlsql.version.VersionCompatibility

import java.util.UUID
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

object ExecSQLApp extends Logging {
  val versions = Seq(">=2.0.1")

  private val connectionPool = new ConcurrentHashMap[String, java.sql.Connection]()


  def executeQueryInDriverWithoutResult(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val stat = ExecSQLApp.connectionPool.get(connName).prepareStatement(sql)
    stat.execute()
  }


  def executeQueryInDriver(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val connect = ExecSQLApp.connectionPool.get(connName)
    val stat = if (connect != null) connect.prepareStatement(sql) else throw new RuntimeException("connection name no found!")
    val rs = stat.executeQuery()
    val res = JDBCUtils.rsToMaps(rs)
    stat.close()
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    val rdd = session.sparkContext.parallelize(res.map(item => {objectMapper.writeValueAsString(item.asJava)}))
    session.read.json(rdd)
  }

  def executeQueryWithDiskCache(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val connect = ExecSQLApp.connectionPool.get(connName)
    val stat = if (connect != null) connect.prepareStatement(sql) else throw new RuntimeException("connection name no found!")
    val rs = stat.executeQuery()

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)


    val fileName = s"${UUID.randomUUID().toString}.json"


    val fs = new Path(".").getFileSystem(HDFSOperatorV2.hadoopConfiguration)
    val homePath = fs.getHomeDirectory()
    val cachePath = new Path(homePath.getName,"tmp","execsql_cache")
    if (!fs.exists(cachePath)) {
      fs.mkdirs(cachePath)
    }
    val dos = fs.create(new Path(cachePath, fileName), true)
    try {

      while (rs.next()) {
        val row = JDBCUtils.rsToMap(rs, JDBCUtils.getRsCloumns(rs))
        val line = objectMapper.writeValueAsString(row.asJava)
        dos.writeBytes(line + "\n")
      }
    } finally {
      dos.close()
      rs.close()
      stat.close()
    }
    session.read.json(new Path(cachePath,fileName).toString)
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
