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
      if ((table.db.isEmpty || table.db.get == "")
        && table.tableType.name != "temp"
        && table.sourceType.getOrElse("") != "delta") return "file";
      table.db.getOrElse(table.tableType.name)
    }

    def getByResource(k: ResourceUnit) = {
      resourceRules.keys.filter { p =>
        val nameMatch = p.name == k.name
        val pathMatch = p.name match {
          case "file" => k.path.toLowerCase.startsWith(p.path.toLowerCase())
          case _ => k.path == p.path
        }
        nameMatch && pathMatch
      }.headOption.
        map(p => resourceRules(p))
    }


    val SUCCESS = TableAuthResult(true, "")

    val result = tables.map { table =>
      val verb = table.operateType.toString.toLowerCase()
      val FAIL = TableAuthResult(false, s"you have no permission to access ${table.db.getOrElse("")}.${table.table.getOrElse("")} with operation ${verb}")

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

      def checkResource(key: ResourceUnit): TableAuthResult = {
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
      }

      // mlsql_system_db.system_info sourceType: _mlsql_
      val tableAuthResult = resourceName(table) match {
        case "file" | "mlsql_system" =>
          val key = ResourceUnit(resourceName(table), table.table.getOrElse(""))
          checkResource(key)
        case _ =>
          (table.tableType.name, table.sourceType.getOrElse("")) match {
            case ("temp", _) => TableAuthResult(true, "")
            case ("system", "_mlsql_") => TableAuthResult(true, "")
            case ("jdbc", _) =>
              val key = ResourceUnit("jdbc", s"${table.db.get}.${table.table.get}")
              checkResource(key)
            case ("hive", "hive") =>
              val key = ResourceUnit("hive", s"${table.db.get}.${table.table.get}")
              checkResource(key)

            case ("hdfs", "delta") =>
              table.table.get.split("/").takeRight(2) match {
                case Array(db, table) =>
                  val key = ResourceUnit("delta", s"${db}.${table}")
                  checkResource(key)
              }
            case (_, _) =>
              TableAuthResult(true, "")
          }

      }
      tableAuthResult
    }

    val denies = result.filter(_.granted == false).map { item =>
      item.msg
    }

    if (denies.nonEmpty) {
      throw new RuntimeException(s"you(${owner}) have no permission to access some resources(click more for detail)\n ${denies.mkString("\n")}")
    }

    result
  }
}

class ByzerSimpleAuthConfig(path: String) {


  def load(): AuthConfig = {
    val files = HDFSOperatorV2.iteratorFiles(path, false)
    val vTemp = files.filter(_.endsWith(".yml")).map { f =>
      val content = HDFSOperatorV2.readFile(f)
      ByzerSimpleAuth.parse(content)
    }
    val v = if (vTemp.isEmpty) {
      AuthConfig("auth.byzer.org/v1", "Auth", List(), List())
    } else {
      vTemp.reduce { (l, r) =>
        l.copy(
          userView = l.userView ++ r.userView,
          resourceView = l.resourceView ++ r.resourceView
        )
      }
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

