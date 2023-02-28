package tech.mlsql.plugins.expandinclude

import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.dsl.ScriptSQLExec._parse
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.Stage
import tech.mlsql.app.CustomController
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.dsl.adaptor.PreProcessIncludeListener
import tech.mlsql.dsl.processor.PreProcessListener
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

class MLSQLExpandInclude extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("expandInclude", classOf[ExpandIncludeController].getName)
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }
}


class ExpandIncludeController extends CustomController {

  def parsePreProcessInclude(input: String) = {
    val listener = ScriptSQLExec.context().execListener

    var wow = input

    var max_preprocess = 10
    var stop = false

    val sqel = listener.asInstanceOf[ScriptSQLExecListener]
    CommandCollection.fill(sqel)
    sqel.setStage(Stage.include)
    while (!stop && max_preprocess > 0) {
      val preProcessListener = new PreProcessIncludeListener(sqel)
      sqel.includeProcessListner = Some(preProcessListener)
      _parse(wow, preProcessListener)

      val newScript = preProcessListener.toScript
      if (newScript == wow) {
        stop = true
      }
      wow = newScript
      max_preprocess -= 1
    }

    val preProcessListener = new PreProcessListener(sqel)
    sqel.preProcessListener = Some(preProcessListener)
    sqel.setStage(Stage.preProcess)
    _parse(wow, preProcessListener)
    wow = preProcessListener.toScript
    wow
  }

  override def run(params: Map[String, String]): String = {
    val sql = params.get("sql")
    val finalSql = sql match {
      case Some(s) =>
         parsePreProcessInclude(s)
      case _ => throw new MLSQLException("export complete need sql param.")
    }
    JSONTool.toJsonStr(Map("success" -> true, "sql" -> finalSql))
  }
}