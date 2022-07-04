package tech.mlsql.plugins.ext.ets.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 31/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLETApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("AthenaExt", classOf[AthenaExt].getName)
    ETRegister.register("AthenaSchemaExt", classOf[AthenaSchemaExt].getName)
    ETRegister.register("FeatureStoreExt", classOf[FeatureStoreExt].getName)
    ETRegister.register("VisualizationExt", classOf[VisualizationExt].getName)
  }


  override def supportedVersions: Seq[String] = {
    MLSQLETApp.versions
  }
}

object MLSQLETApp {
  val versions = Seq(">=2.0.1")
}