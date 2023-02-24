package tech.mlsql.plugins.auth.simple.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 24/2/2023 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerSimpleAuthApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ByzerAuthAdmin", classOf[ByzerAuthAdmin].getName)
    CommandCollection.refreshCommandMapping(Map("simpleAuth" ->
      """
        |run command as ByzerAuthAdmin.`` where parameters='''{:all}'''
        |""".stripMargin))
  }

  override def supportedVersions: Seq[String] = {
    ByzerSimpleAuthApp.versions
  }

}

object ByzerSimpleAuthApp {
  val versions = Seq(">=2.0.1")
}
