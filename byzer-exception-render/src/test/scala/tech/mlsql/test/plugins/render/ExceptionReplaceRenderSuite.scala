package tech.mlsql.test.plugins.render

import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.funsuite.AnyFunSuite
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import tech.mlsql.job.JobManager
import tech.mlsql.plugins.render.ExceptionReplaceRender
import tech.mlsql.runtime.plugins.request_cleaner.RequestCleanerManager
import tech.mlsql.test.BasicMLSQLConfig

class ExceptionReplaceRenderSuite extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig {
  test("ExceptionReplaceRender should work") {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime  =>
      JobManager.init(runtime.sparkSession, initialDelay = 2 , checkTimeInterval = 3)
      // Run a script to setup the byzer-lang's context
      val script = """!show version;""".stripMargin
      val params = Map( "__PARAMS__" ->  """{"async":"true"}""", "show_stack" -> "true")
      executeCode(runtime, script, params)

      val render = new ExceptionReplaceRender
      val e = new RuntimeException("MLSQL Parser error in [line 1 column 2]")
      assert(render.is_match(e))
      assert("MLSQL Parser error" == render.format(e))
      assert( render.getMatchExp().isEmpty )
      val context = ScriptSQLExec.context()
      // Mark job finished
      context.execListener.addEnv("__MarkAsyncRunFinish__", "true")
      // Clean temp data
      RequestCleanerManager.call()
    }
  }

}
