package tech.mlsql.plugins.delta.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.delta.ets.DeltaTable
import tech.mlsql.version.VersionCompatibility

class ByzerDelta extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("DeltaTable", classOf[DeltaTable].getName)
    CommandCollection.refreshCommandMapping(Map("deltaTable" ->
      """
        |run command as DeltaTable.`` where parameters='''{:all}'''
        |""".stripMargin))
  }


  override def supportedVersions: Seq[String] = {
    ByzerDelta.versions
  }
}

object ByzerDelta {
  val versions = Seq(">=2.0.1")
}
