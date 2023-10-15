package tech.mlsql.plugins.llm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions=>F}
import streaming.core.datasource.MLSQLRegistry
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.llm.qa.ByzerLLMQABuilder
import tech.mlsql.plugins.llm.tools.ModelAdmin
import tech.mlsql.version.VersionCompatibility

import java.lang.reflect.Modifier


/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
class LLMApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    MLSQLConfig.run()
    ETRegister.register("LLM", classOf[LLM].getName)
    ETRegister.register("Retrieval", classOf[Retrieval].getName)
    ETRegister.register("LLMQABuilder", classOf[ByzerLLMQABuilder].getName)
    //    CommandCollection.refreshCommandMapping(Map("llm" ->
    //      """
    //        |run command as LLM.`` where parameters='''{:all}'''
    //        |""".stripMargin))
    registerDS(classOf[MLSQLModel].getName)
    registerDS(classOf[MLSQLModelFast].getName)

    if (ScriptSQLExec.context() != null) {
      registerUDF("tech.mlsql.plugins.llm.LLMUDF", ScriptSQLExec.context().execListener.sparkSession)
    }
    registerUDF("tech.mlsql.plugins.llm.LLMUDF", PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession)

    ETRegister.register("ModelAdmin", classOf[ModelAdmin].getName)
    CommandCollection.refreshCommandMapping(Map("byzerllm" ->
      """
        |run command as ModelAdmin.`` where parameters='''{:all}'''
        |""".stripMargin))

  }


  def registerUDF(clzz: String, session: SparkSession) = {
    
    ClassLoaderTool.classForName(clzz).getMethods.foreach { f =>
      try {
        if (Modifier.isStatic(f.getModifiers)) {
          f.invoke(null, session.udf)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
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