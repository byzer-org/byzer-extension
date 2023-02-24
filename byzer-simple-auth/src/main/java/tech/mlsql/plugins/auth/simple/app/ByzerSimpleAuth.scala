package tech.mlsql.plugins.auth.simple.app

import org.apache.commons.io.FileUtils
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

import java.io.File
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
      return "file"
    }

    def getByResource(k: ResourceUnit) = {
      resourceRules.keys.filter(p => p.name == k.name && k.path.toLowerCase.startsWith(p.path.toLowerCase())).
        headOption.
        map(p => resourceRules(p))
    }


    val SUCCESS = TableAuthResult(true, "")

    val result = tables.map { table =>
      val verb = table.operateType.toString.toLowerCase()
      val FAIL = TableAuthResult(false, s"you have no permission to ${verb} ${table.table.getOrElse("")}")

      def matchVerb(verbs: List[String], verb: String): Boolean = {
        if (verbs.contains("*")) return true
        verbs.contains(verb)
      }

      def checkRule(rule: ResourceRule): Boolean = {
        val allowUsers = rule.users.allows
        val deniedUsers = rule.users.denies

        if (!allowUsers.isEmpty) {
          return allowUsers.contains(owner)
        }
        return !deniedUsers.contains(owner)
      }


      val tableAuthResult = resourceName(table) match {
        case "file" =>
          val key = ResourceUnit(resourceName(table), table.table.getOrElse(""))
          val value = getByResource(key)
          if (value.isDefined) {
            val rules = value.get
            val result = rules.filter(rule => matchVerb(rule.verbs, verb)).exists { rule =>
              checkRule(rule)
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
    import io.circe.generic.auto._
    import io.circe.yaml
    val conent = FileUtils.readFileToString(new File(path), "UTF-8")
    val json = yaml.parser.parse(conent)
    val value = json.right.get.as[AuthConfig]
    value.right.get
  }
}

object ByzerSimpleAuth {
  lazy val config = {
    val v = new AtomicReference[AuthConfig]()
    val session = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
    val authFile = session.conf.get("spark.mlsql.auth.simple.dir", "")
    if (!authFile.isEmpty) {
      val c = new ByzerSimpleAuthConfig(authFile)
      v.set(c.load())
    }
    v
  }

  def reload = {
    val session = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
    val authFile = session.conf.get("spark.mlsql.auth.simple.dir", "")
    if (!authFile.isEmpty) {
      val c = new ByzerSimpleAuthConfig(authFile)
      config.set(c.load())
    }
  }

}

