package tech.mlsql.plugins.ext.ets.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 31/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLETApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("AthenaExt", classOf[AthenaExt].getName)
    ETRegister.register("AthenaSchemaExt", classOf[AthenaSchemaExt].getName)
    ETRegister.register("LSCommand", classOf[LSCommand].getName)
    ETRegister.register("ExistsCommand", classOf[ExistsCommand].getName)
    ETRegister.register("FileStatusCommand", classOf[FileStatusCommand].getName)
    CommandCollection.refreshCommandMapping(Map("ls" -> """ run command as LSCommand.`` where path="{0}" """))
    CommandCollection.refreshCommandMapping(Map("exists" -> """ run command as ExistsCommand.`` where path="{0}" """))
    CommandCollection.refreshCommandMapping(Map("fileStatus" -> """ run command as FileStatusCommand.`` where path="{0}" """))
  }


  override def supportedVersions: Seq[String] = {
    MLSQLETApp.versions
  }
}

object MLSQLETApp {
  val versions = Seq(">=2.0.1")
}