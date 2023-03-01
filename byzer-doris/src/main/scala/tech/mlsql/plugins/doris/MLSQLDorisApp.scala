package tech.mlsql.plugins.doris

import streaming.core.datasource.MLSQLRegistry
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

class MLSQLDorisApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    registerDS(classOf[MLSQLDoris].getName)
  }

  def registerDS(name: String) = {
    val dataSource = ClassLoaderTool.classForName(name).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
  }

  override def supportedVersions: Seq[String] = {
    MLSQLDorisApp.versions
  }

}

object MLSQLDorisApp {
  val versions = Seq(">=2.0.1")
}