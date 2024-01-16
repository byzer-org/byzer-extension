package tech.mlsql.plugins.ds

import streaming.core.datasource.MLSQLRegistry
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility


/**
 * 13/11/23 naopyani(naopyani@gmail.com)
 */
class MLSQLSASApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    val clzz = classOf[MLSQLSAS].getName
    logInfo(s"Load ds: ${clzz}")
    val dataSource = ClassLoaderTool.classForName(clzz).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
  }

  override def supportedVersions: Seq[String] = {
    MLSQLSASApp.versions
  }

}

object MLSQLSASApp extends Logging {
  val versions = Seq(">=2.0.1")

}

