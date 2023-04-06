package tech.mlsql.plugins.llm

import streaming.core.datasource.MLSQLRegistry
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
class LLMApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("LLM", classOf[LLM].getName)
    CommandCollection.refreshCommandMapping(Map("llm" ->
      """
        |run command as LLM.`` where parameters='''{:all}'''
        |""".stripMargin))
    registerDS(classOf[MLSQLModel].getName)

  }

  def registerDS(name: String) = {
    val dataSource = ClassLoaderTool.classForName(name).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
  }


  override def supportedVersions: Seq[String] = {
    LLMApp.versions
  }
}

object LLMApp {
  val versions = Seq(">=2.0.1")
}