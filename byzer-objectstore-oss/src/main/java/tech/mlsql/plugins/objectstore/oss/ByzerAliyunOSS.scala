package tech.mlsql.plugins.objectstore.oss

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 23/9/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerAliyunOSS extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    try {
      Class.forName("org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      logInfo("V3:Success to load jar for Aliyun OSS FileSystem ")

    } catch {
      case e: Exception =>
        logError("Fail to load jar for Aliyun OSS FileSystem")
    }

  }


  override def supportedVersions: Seq[String] = {
    ByzerAliyunOSS.versions
  }
}

object ByzerAliyunOSS {
  val versions = Seq(">=2.0.1")
}