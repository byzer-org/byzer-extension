package tech.mlsql.plugins.ext.ets.app

import org.apache.hadoop.conf.Configuration

/**
 * 20/12/2022 hellozepp(lisheng.zhanglin@163.com)
 */
trait FileCommonFunctions {

  def rewriteHadoopConfiguration(hadoopConfiguration: Configuration, config: Map[String, String]): Unit = {
    config.filter(item => item._1.startsWith("spark.hadoop") || item._1.startsWith("fs."))
      .foreach { item =>
        if (item._1.endsWith("fs.defaultFS")) {
          throw new RuntimeException("fs.defaultFS is not allowed to be modified at runtime! " +
            "Avoid causing the file system used by the system to be altered.")
        }
        if (item._1.startsWith("spark.hadoop")) {
          hadoopConfiguration.set(item._1.replaceAll("spark.hadoop.", ""), item._2)
        } else {
          hadoopConfiguration.set(item._1, item._2)
        }
      }
  }
}
