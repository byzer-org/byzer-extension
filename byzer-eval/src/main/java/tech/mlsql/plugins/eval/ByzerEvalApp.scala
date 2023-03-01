package tech.mlsql.plugins.eval

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 28/2/2023 WilliamZhu(allwefantasy@gmail.com)
 */
class ByzerEvalApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ByzerEval", classOf[ByzerEval].getName)
    CommandCollection.refreshCommandMapping(Map("eval" ->
      """
        |run command as ByzerEval.`` where parameters='''{:all}'''
        |""".stripMargin))
  }

  override def supportedVersions: Seq[String] = {
    ByzerEvalApp.versions
  }

}

object ByzerEvalApp {
  val versions = Seq(">=2.0.1")
}
