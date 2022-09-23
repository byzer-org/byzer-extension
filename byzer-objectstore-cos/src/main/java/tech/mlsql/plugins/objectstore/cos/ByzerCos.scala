package tech.mlsql.plugins.objectstore.cos

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 23/9/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerCos extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    try {
      Class.forName("org.apache.hadoop.fs.CosFileSystem")
      logInfo("Success to load jar for Cos FileSystem ")
    } catch {
      case e: Exception =>
        logError("Fail to load jar for Cos FileSystem")
    }

  }


  override def supportedVersions: Seq[String] = {
    ByzerCos.versions
  }
}

object ByzerCos {
  val versions = Seq(">=2.0.1")
}