package tech.mlsql.plugins.llm

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.util.TaskCompletionListener
import tech.mlsql.tool.HDFSOperatorV2

import java.io.InputStream
import java.util.UUID

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLModelFast(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    val ROW_BLOCK_SIZE = 1024 * 64

    val context = ScriptSQLExec.contextGetOrForTest()
    val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)


    val session = config.df.get.sparkSession

    val rdd = session.sparkContext.makeRDD(Seq(1), 1).mapPartitionsWithIndex { (index, iter) =>
      iter.toList
      val fileSystem = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
      val uuid = UUID.randomUUID().toString


      val tempFile = new Path(fileSystem.getWorkingDirectory(), s"${uuid}.staging")

      println(s"temp_file:${tempFile}")

      TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          fileSystem.delete(tempFile, true)
        }
      })

      val dos = fileSystem.create(tempFile, true)
      TarfileUtilsWrapper2.createTarFileStream(dos, targetPath, new FileNameFilter {
        override def accept(fileName: String): Boolean = {
          !new Path(fileName).getName.equals(".git")
        }
      })

      def tarFileToChunks(tarIn: InputStream): Iterator[Row] = {
        new Iterator[Row] {
          var start = 0l

          override def hasNext: Boolean = {
            val ifHasNext = tarIn.available() > 0
            if (!ifHasNext) {
              tarIn.close()
            }
            ifHasNext
          }

          override def next(): Row = {
            val buf = new Array[Byte](ROW_BLOCK_SIZE)
            val len = tarIn.read(buf)
            val s = buf.take(len)
            val row = Row.fromSeq(Seq(start, len.toLong, s))
            start += len
            row
          }
        }
      }

      val f = fileSystem.open(tempFile)
      tarFileToChunks(f)
    }
    val newDF = session.createDataFrame(rdd, StructType(Array(
      StructField("start", LongType),
      StructField("offset", LongType),
      StructField("value", BinaryType))))
    newDF
  }


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    return null
  }


  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "model"

  override def shortFormat: String = "model"

}

