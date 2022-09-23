package tech.mlsql.plugins.objectstore.obs

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 23/9/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerObs extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    try {
      Class.forName("org.apache.hadoop.fs.obs.OBSFileSystem")
      logInfo("Success to load jar for OBS FileSystem ")
    } catch {
      case e: Exception =>
        logError("Fail to load jar for OBS FileSystem")
    }

  }


  override def supportedVersions: Seq[String] = {
    ByzerObs.versions
  }
}

object ByzerObs {
  val versions = Seq(">=2.0.1")
}