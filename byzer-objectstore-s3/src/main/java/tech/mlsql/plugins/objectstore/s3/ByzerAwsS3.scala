package tech.mlsql.plugins.objectstore.s3

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 23/9/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerAwsS3 extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    try {
      Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")
      logInfo("Success to load jar for Aliyun OSS FileSystem ")
    } catch {
      case e: Exception =>
        logError("Fail to load jar for Aliyun OSS FileSystem")
    }

  }


  override def supportedVersions: Seq[String] = {
    ByzerAwsS3.versions
  }
}

object ByzerAwsS3 {
  val versions = Seq(">=2.0.1")
}