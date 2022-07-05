package tech.mlsql.plugins.openmldb

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 5/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("FeatureStoreExt", classOf[FeatureStoreExt].getName)
  }


  override def supportedVersions: Seq[String] = {
    MLSQLETApp.versions
  }
}

object MLSQLETApp {
  val versions = Seq(">=2.0.1")
}
