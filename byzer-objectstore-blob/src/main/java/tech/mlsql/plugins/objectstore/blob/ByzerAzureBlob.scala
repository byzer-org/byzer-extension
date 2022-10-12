package tech.mlsql.plugins.objectstore.blob

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 23/9/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerAzureBlob extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    try {
      Class.forName("org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      logInfo("Success to load jar for native Azure FileSystem ")
    } catch {
      case e: Exception =>
        logError("Fail to load jar for native Azure FileSystem")
    }

  }


  override def supportedVersions: Seq[String] = {
    ByzerAzureBlob.versions
  }
}

object ByzerAzureBlob {
  val versions = Seq(">=2.0.1")
}