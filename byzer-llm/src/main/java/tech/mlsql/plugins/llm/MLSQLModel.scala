package tech.mlsql.plugins.llm

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{functions => f, _}
import org.kamranzafar.jtar.{TarEntry, TarInputStream}
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.tool.{HDFSOperatorV2, SparkTarfileUtil}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLModel(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)
    val modelByteSize = JavaUtils.byteStringAsBytes(config.config.getOrElse("modelSize", "20g"))

    val ROW_BLOCK_SIZE = 1024 * 64
    val PARTITION_NUM = config.config.getOrElse("partitionNum", "1").toLong

    val enableDebug = config.config.getOrElse("enableDebug", "false").toBoolean

    val bufferNum = config.config.getOrElse("bufferNum", "100").toInt
    val bufferTimeout = JavaUtils.timeStringAsSec(config.config.getOrElse("bufferTimeout", "3m"))

    val blocks = Math.ceil(modelByteSize.toDouble / ROW_BLOCK_SIZE).toLong
    val blocksNumPerPartition = Math.ceil(blocks.toDouble / PARTITION_NUM).toLong

    // to caculate the (start,end) of every partition
    val partitionStartEnd = ArrayBuffer[(Long, (Long, Long))]()
    (0 until PARTITION_NUM.toInt).foreach { i =>
      val start = i * blocksNumPerPartition * ROW_BLOCK_SIZE
      val end = if (i == PARTITION_NUM - 1) -1 else (i + 1) * blocksNumPerPartition * ROW_BLOCK_SIZE
      partitionStartEnd += ((i, (start, end)))
    }

    val session = config.df.get.sparkSession

    val rdd = session.sparkContext.makeRDD(partitionStartEnd, 1).partitionBy(new Partitioner {
      override def numPartitions: Int = partitionStartEnd.size

      override def getPartition(key: Any): Int = {
        val i = key.asInstanceOf[Long]
        i.toInt
      }
    }).mapPartitionsWithIndex { (index, iter) =>
      val items = iter.toList
      require(items.length == 1, s"The partition should contains only one row which is (start,end): " +
        s"now modelByteSize:${modelByteSize} blocks:${blocks} blocksNumPerPartition:${blocksNumPerPartition} " +
        s"partition[${index}]: ${items.map(item => item._2._1 + "-" + item._2._2).mkString(",")}")

      val filterStart = items.head._2._1
      val filterEnd = items.head._2._2

      var done = new AtomicBoolean(false)
      var start = 0L

      val stack = new LinkedBlockingQueue[(Long, Long, Array[Byte])](bufferNum)

      val bytesOutputStream = new YieldByteArrayOutputStream(ROW_BLOCK_SIZE, (buf: Array[Byte], count: Int, _done) => {
        if (count > 0) {
          if (start >= filterStart && (filterEnd == -1 || (start + count) < filterEnd)) {
            if (enableDebug) {
              println(s"offer partition:${index} start:${start} offset:${count}")
            }
            val success = stack.offer((start, count, java.util.Arrays.copyOf(buf, count)), bufferTimeout, TimeUnit.SECONDS)
            if (!success) {
              throw new RuntimeException(s"offer to stack failed, partition:${index} start:${start} offset:${count}")
            }
          }
          if (enableDebug) {
            println(s"iterate partition:${index} start:${start} offset:${count}")
          }
          start = start + count
        }
        if (_done) {
          done.set(_done)
        }
      })

      new Thread(new Runnable {
        override def run(): Unit = TarfileUtilsWrapper.createTarFileStream(bytesOutputStream, targetPath, new FileNameFilter {
          override def accept(fileName: String): Boolean = {
            !new Path(fileName).getName.equals(".git")
          }
        })
      }).start()


      new Iterator[Row]() {
        override def hasNext: Boolean = {
          while (!done.get() && stack.size() == 0) {
            Thread.sleep(100)
          }
          !done.get() || stack.size() > 0
        }

        override def next(): Row = {
          val item = stack.poll(bufferTimeout, TimeUnit.SECONDS)
          if (enableDebug) {
            println(s"poll to runtime, partition:${index} start:${item._1} offset:${item._2}")
          }
          Row.fromSeq(Seq(item._1, item._2, item._3))
        }
      }
    }
    val newDF = session.createDataFrame(rdd, StructType(Array(
      StructField("start", LongType),
      StructField("offset", LongType),
      StructField("value", BinaryType))))
    newDF
  }


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    val context = ScriptSQLExec.contextGetOrForTest()
    val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)
    val rdd = config.df.get.repartition(1).sortWithinPartitions(f.col("start").asc).rdd
    assert(rdd.partitions.length == 1, "rdd partition num should be 1")
    rdd.foreachPartition { iter =>
      if (!iter.hasNext) Seq[Row]().toIterator
      else {
        val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
        val inputStream = SparkTarfileUtil.buildInputStreamFromIterator(iter)

        val fileNames = new ArrayBuffer[String]()
        val tarInputStream = new TarInputStream(inputStream)

        var entry: TarEntry = tarInputStream.getNextEntry
        while (entry != null) {
          fileNames += entry.getName
          val targetFilePath = new Path(PathFun(targetPath).add(entry.getName).toPath)

          if (!fs.exists(targetFilePath.getParent)) {
            fs.mkdirs(targetFilePath.getParent)
          }
          if (!entry.isDirectory) {
            val targetFile = fs.create(targetFilePath, true)
            IOUtils.copy(tarInputStream, targetFile)
            targetFile.close()
          }

          entry = tarInputStream.getNextEntry
        }
        tarInputStream.close()
        inputStream.close()
        fileNames.toIterator
      }

    }
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "model2"

  override def shortFormat: String = "model2"

}