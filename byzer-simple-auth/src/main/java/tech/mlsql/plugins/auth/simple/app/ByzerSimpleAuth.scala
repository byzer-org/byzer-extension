package tech.mlsql.plugins.auth.simple.app


import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.tool.HDFSOperatorV2

import java.util.concurrent.atomic.AtomicReference


class ByzerSimpleAuth extends TableAuth with Logging with WowLog {

  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    val configRef = ByzerSimpleAuth.config

    if (configRef.get() == null) {
      logError("spark.mlsql.auth.simple.dir is not set")
      return List(TableAuthResult(false, "spark.mlsql.auth.simple.dir is not set"))
    }

    val config = configRef.get
    // for now only support resource view

    val resourceRules = config.resourceView.flatMap { item =>
      item.metadata.resources.get.map { resource =>
        (resource, item.rules)
      }
    }.toMap

    def resourceName(table: MLSQLTable): String = {
      if (table.db.isEmpty && table.table.map(_.startsWith("/")).isDefined) return "file";
      table.db.get
    }

    def getByResource(k: ResourceUnit) = {
      resourceRules.keys.filter(p => p.name == k.name && k.path.toLowerCase.startsWith(p.path.toLowerCase())).
        headOption.
        map(p => resourceRules(p))
    }


    val SUCCESS = TableAuthResult(true, "")

    val result = tables.map { table =>
      val verb = table.operateType.toString.toLowerCase()
      val FAIL = TableAuthResult(false, s"you have no permission to ${table.db.getOrElse("")}.${table.table.getOrElse("")} with operation ${verb}")

      def matchVerb(verbs: List[String], verb: String): Boolean = {
        if (verbs.contains("*")) return true
        verbs.contains(verb)
      }

      def checkRule(rule: ResourceRule): Boolean = {
        val allowUsers = rule.users.allows
        val deniedUsers = rule.users.denies

        if (!allowUsers.isEmpty) {
          return allowUsers.exists(_.name == owner)
        }
        return !deniedUsers.exists(_.name == owner)
      }


      val tableAuthResult = resourceName(table) match {
        case "file" | "mlsql_system" =>
          val key = ResourceUnit(resourceName(table), table.table.getOrElse(""))
          val value = getByResource(key)
          if (value.isDefined) {
            val rules = value.get
            val result = rules.filter(rule => matchVerb(rule.rule.verbs, verb)).exists { rule =>
              checkRule(rule.rule)
            }
            if (result) {
              SUCCESS
            } else {
              FAIL
            }
          } else {
            TableAuthResult(true, "")
          }
        case _ => TableAuthResult(true, "")
      }
      tableAuthResult
    }
    result
  }
}

class ByzerSimpleAuthConfig(path: String) {


  def load(): AuthConfig = {
    val files = HDFSOperatorV2.iteratorFiles(path, false)
    val v = files.map { f =>
      val content = HDFSOperatorV2.readFile(f)
      ByzerSimpleAuth.parse(content)
    }.reduce { (l, r) =>
      l.copy(
        userView = l.userView ++ r.userView,
        resourceView = l.resourceView ++ r.resourceView
      )
    }
    v
  }
}

object ByzerSimpleAuth {
  var v_spark_mlsql_auth_simple_dir: Option[String] = None
  lazy val config = {
    val v = new AtomicReference[AuthConfig]()
    val authFile = if (v_spark_mlsql_auth_simple_dir.isEmpty) {
      val session = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
      session.conf.get("spark.mlsql.auth.simple.dir", "")
    } else {
      v_spark_mlsql_auth_simple_dir.get
    }
    if (!authFile.isEmpty) {
      val c = new ByzerSimpleAuthConfig(authFile)
      v.set(c.load())
    }
    v
  }

  def reload = {
    val authFile = if (v_spark_mlsql_auth_simple_dir.isEmpty) {
      val session = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
      session.conf.get("spark.mlsql.auth.simple.dir", "")
    } else {
      v_spark_mlsql_auth_simple_dir.get
    }
    if (!authFile.isEmpty) {
      val c = new ByzerSimpleAuthConfig(authFile)
      config.set(c.load())
    }
  }

  def parse(content: String): AuthConfig = {

    //    val cc = Json_Yaml.object_to_json()
    //    JSONTool.parseJson[AuthConfig](cc)
    Json_Yaml.yaml_to_object(content)
  }

}

