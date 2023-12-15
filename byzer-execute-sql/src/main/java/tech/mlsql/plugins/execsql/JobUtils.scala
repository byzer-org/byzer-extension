package tech.mlsql.plugins.execsql

import java.util.UUID
import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, TimeUnit}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat.forPattern
import streaming.core.datasource.JDBCUtils
import streaming.core.datasource.JDBCUtils.formatOptions
import tech.mlsql.common.utils.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.tool.HDFSOperatorV2
import com.alibaba.druid.util.{JdbcConstants, JdbcUtils}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConverters._

/**
 * 11/2/23 WilliamZhu(allwefantasy@gmail.com)
 */
class JobUtils extends Logging {
}

case class ConnectionHolder(val name: String, val options: Map[String, String], val connection: java.sql.Connection)

object JobUtils extends Logging {
  private val connectionPool = new ConcurrentHashMap[String, ConnectionHolder]()
  private val cacheFiles = CacheBuilder.newBuilder().
    maximumSize(100000).removalListener(new RemovalListener[String, java.util.List[String]]() {
      override def onRemoval(notification: RemovalNotification[String, util.List[String]]): Unit = {
        val files = notification.getValue
        if (files != null) {
          val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
          files.asScala.foreach { file =>
            try {
              logInfo(s"remove cache file ${file}")
              fs.delete(new Path(file), true)
              // delete the .crc file
              deleteCRCFile(fs,file)
            } catch {
              case e: Exception =>
                logError(s"remove cache file ${file} failed", e)
            }

          }
        }
      }
    }).
    expireAfterWrite(2, TimeUnit.DAYS).
    build[String, java.util.List[String]]()

  // by default we use the hadoop.tmp.dir to store the cache file
  private lazy val cacheDir = {
    var tmpPath = HDFSOperatorV2.hadoopConfiguration.get("hadoop.tmp.dir")
    if (tmpPath == null || tmpPath.isEmpty) {
      logInfo(s"hadoop.tmp.dir is not set")
      tmpPath = "/tmp"
    }

    val cachePath = new Path(tmpPath, "execsql_cache")
    val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
    if (!fs.exists(cachePath)) {
      fs.mkdirs(cachePath)
    }
    logInfo(s"cache data in ${cachePath.toString} tmpPath:${tmpPath}")
    val v = new AtomicReference[String]()
    v.set(cachePath.toString)
    v
  }

  // the cache file will be cleaned by user manually, when the user remove
  // the connection, we will remove the cache file either.
  // This clean thread is used to make sure when the user forget to clean or the
  // system restart which caused the loss of the cache file track information, so
  // we can still clean the old cache file.
  private val cleanThread = Executors.newSingleThreadScheduledExecutor()
  cleanThread.schedule(new Runnable {
    override def run(): Unit = {
      try {
        logInfo("try to clean old files...")
        if (cacheDir.get() != null) {
          cleanOldFiles(new Path(cacheDir.get()))
        }
      } catch {
        case e: Exception =>
          logError("clean old files failed", e)
      }
    }
  }, 30, java.util.concurrent.TimeUnit.MINUTES)

  def executeQueryInDriverWithoutResult(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val connect = fetchConnection(connName)
    val stat = if (connect != null) connect.connection.prepareStatement(sql) else throw new RuntimeException(s"connection ${connName} no found!")
    stat.execute()
    stat.close()
  }

  // try catch without exception
  def try_close(func: () => Unit) = {
    try {
      func()
    } catch {
      case e: Exception =>
        logError("execute func failed", e)
    }
  }

  def deleteCRCFile(fs:FileSystem,path: String) = {
    // get file name from path
    val fileName = path.split("/").last
    val crcFileName = "." + path + ".crc"
    val crcFile = path.stripSuffix(s"${fileName}") + crcFileName
    fs.delete(new Path(crcFile), true)
  }


  def executeQueryInDriver(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val connect = fetchConnection(connName)
    val stat = if (connect != null) connect.connection.prepareStatement(sql) else throw new RuntimeException(s"connection ${connName} no found!")
    val rs = stat.executeQuery()
    val res = JDBCUtils.rsToMaps(rs)
    stat.close()
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    val rdd = session.sparkContext.parallelize(res.map(item => {
      objectMapper.writeValueAsString(item.asJava)
    }))
    session.read.json(rdd)
  }

  private def fetchConnection(connName: String) = {
    val connectionHolder = JobUtils.connectionPool.get(connName)
    if (connectionHolder == null) throw new RuntimeException(s"connection ${connName} no found!")
    val connect = connectionHolder.connection

    if (connect.isClosed || !connect.isValid(5)) {
      logInfo(s"connection ${connName} is closed or invalid, try to reconnect")
      removeConnection(connName)
      newConnection(connName, connectionHolder.options)
      val connectionHolder2 = JobUtils.connectionPool.get(connName)
      connectionHolder2
    } else {
      connectionHolder
    }

  }

  def executeQueryWithDiskCache(session: SparkSession, connName: String, sql: String) = {
    import scala.collection.JavaConverters._
    val connectionHolder = fetchConnection(connName)
    val connect = connectionHolder.connection
    connect.setAutoCommit(false)

    val isMySqlDriver = JdbcUtils.isMySqlDriver(connectionHolder.options("driver"))


    val stat = if (connect != null) connect.prepareStatement(sql) else throw new RuntimeException(s"connection ${connName} no found!")
    if (isMySqlDriver) {
      // Integer.MIN_VALUE
      stat.setFetchSize(-2147483648)
    }
    val rs = stat.executeQuery()

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule())

    // get the time with format yyyy-MM-dd-HH-mm-ss
    val time = DateTime.now().toString("yyyyMMddHHmmss")
    val fileName = s"${UUID.randomUUID().toString}-${time}.json"


    val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
    val dos = fs.create(new Path(cacheDir.get(), fileName), true)
    try {

      while (rs.next()) {
        val row = JDBCUtils.rsToMap(rs, JDBCUtils.getRsCloumns(rs))
        val line = objectMapper.writeValueAsString(row.asJava)
        dos.write((line + "\n").getBytes("UTF-8"))
      }
    } finally {
      try_close(() => {
        dos.close()
      })
      try_close(() => {
        rs.close()
      })
      try_close(() => {
        stat.close()
      })
    }

    // put the cache file path into cacheFiles, so when the user
    // remove the connection, we can delete the cache file
    val files = cacheFiles.get(connName, new Callable[java.util.List[String]] {
      override def call(): java.util.List[String] = {
        new util.LinkedList[String]()
      }
    })
    files.add(new Path(cacheDir.get(), fileName).toString)
    session.read.json(new Path(cacheDir.get(), fileName).toString)
  }

  def cleanOldFiles(path: Path): Unit = {
    val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
    // list all files in path and remove them according to the time
    val files = fs.listStatus(path)
    val now = DateTime.now()
    for (file <- files) {
      val fileName = file.getPath.getName
      if (fileName.endsWith(".json")) {
        val time = fileName.split("-").last.stripSuffix(".json")
        val fileTime = DateTime.parse(time, forPattern("yyyyMMddHHmmss"))
        val diff = now.getMillis - fileTime.getMillis
        // clean files 48h ago
        if (diff > 1000 * 60 * 60 * 24 * 2) {
          logInfo(s"clean file ${file.getPath.toString}")
          fs.delete(file.getPath, true)
          // delete the .crc file
          deleteCRCFile(fs,file.getPath.toString)
        }
      }

    }

  }

  def newConnection(name: String, options: Map[String, String]) = synchronized {
    val driver = options("driver")
    val url = options("url")
    logInfo(s"create connection ${name} ${url}")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, formatOptions(options))
    if (JobUtils.connectionPool.containsKey(name)) {
      removeConnection(name)
    }
    JobUtils.connectionPool.put(name, ConnectionHolder(name, options, connection))
    JobUtils.connectionPool.get(name)
  }

  def removeConnection(name: String) = synchronized {
    if (JobUtils.connectionPool.containsKey(name)) {
      val connection = JobUtils.connectionPool.get(name).connection
      try_close(() => {
        connection.close()
      })
      JobUtils.connectionPool.remove(name)
      cacheFiles.invalidate(name)
    }
  }
}
