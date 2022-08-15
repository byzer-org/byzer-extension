package tech.mlsql.plugins.visualization

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 5/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerVisualizationApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("VisualizationExt", classOf[VisualizationExt].getName)
    CommandCollection.refreshCommandMapping(Map("visualize" ->
      """
        |run command as VisualizationExt.`` where parameters='''{:all}'''
        |""".stripMargin))
  }


  override def supportedVersions: Seq[String] = {
    MLSQLETApp.versions
  }
}

object MLSQLETApp {
  val versions = Seq(">=2.0.1")
}