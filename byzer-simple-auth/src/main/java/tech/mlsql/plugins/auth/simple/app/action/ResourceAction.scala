package tech.mlsql.plugins.auth.simple.app.action


import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.PathFun
import tech.mlsql.common.utils.Md5
import tech.mlsql.common.utils.shell.command.ParamsUtil
import tech.mlsql.plugins.auth.simple.app.{AuthConfig, ByzerSimpleAuth, Json_Yaml, Metadata, ResourceBlock, ResourceRule, ResourceRuleWrapper, ResourceUnit, User, UserBlock, UserUnit}
import tech.mlsql.tool.HDFSOperatorV2

/**
 * !authSimple resource add  _  -type file -path "s3a://xxxx" -allows testRole:jack -denies testRole:tom
 */

object ResourceActionHelper {

  def getDir = {
    val session = ScriptSQLExec.contextGetOrForTest().execListener.sparkSession
    session.conf.get("spark.mlsql.auth.simple.dir", "")
  }

  def createAuthConfig(t: String, path: String, allows: String, denies: String) = {
    val authConfig = AuthConfig("auth.byzer.org/v1", "Auth", List[UserBlock](),
      List(
        ResourceBlock(
          metadata = Metadata(users = None, resources = Option(List(ResourceUnit(t, path)))),
          rules = List(
            ResourceRuleWrapper(
              rule = ResourceRule(
                verbs = List("*"),
                users = User(
                  allows = allows.split(",").filter(!_.isEmpty).map { f =>
                    f.trim.split(":") match {
                      case Array(role, user) => UserUnit(user, role)
                      case Array(user) => UserUnit(user, "")
                    }
                  }.toList,
                  denies = denies.split(",").filter(!_.isEmpty).map { f =>
                    f.trim.split(":") match {
                      case Array(role, user) => UserUnit(user, role)
                      case Array(user) => UserUnit(user, "")
                    }
                  }.toList)
              )
            )
          )
        )
      )
    )
    authConfig

  }
}

class ResourceQueryAction {
  // !simpleAuth resource query _ -type file -path "s3a://xxxx"
  def run(args: List[String]): String = {
    assert(args.headOption.isDefined, "parameters must be provided")
    assert(args.head == "_", "the first parameter must be _")
    val parser = new ParamsUtil(args.drop(1).mkString(" "))

    val t = parser.getParam("type")
    val path = parser.getParam("path")

    val items = ByzerSimpleAuth.config.get().resourceView.filter(_.metadata.resources.getOrElse(List()).exists(f => f.name == t && f.path == path)).toList
    Json_Yaml.object_to_yaml(ByzerSimpleAuth.config.get().copy(resourceView = items, userView = List()))
  }
}

class ResourceAddAction {
  def run(args: List[String]): String = {
    assert(args.headOption.isDefined, "parameters must be provided")

    if(args.head != "_"){
      val yaml = args.head
      if (!yaml.isEmpty) {
        // to test the yaml string format
        Json_Yaml.yaml_to_object(yaml)
        HDFSOperatorV2.saveFile(ResourceActionHelper.getDir, s"${Md5.md5Hash(yaml)}.yml", List(("", yaml)).toIterator);
        return "success"
      }
    }

    assert(args.head == "_", "the first parameter must be _")
    val parser = new ParamsUtil(args.drop(1).mkString(" "))

    val t = parser.getParam("type")
    val path = parser.getParam("path")
    val allows = parser.getParam("allows", "")
    val denies = parser.getParam("denies", "")

    val authConfig = ResourceActionHelper.createAuthConfig(t, path, allows, denies)

    val content = Json_Yaml.object_to_yaml(authConfig)
    val dir = ResourceActionHelper.getDir
    HDFSOperatorV2.saveFile(dir, s"${Md5.md5Hash(content)}.yml", List(("", content)).toIterator);
    "success"
  }
}


class ResourceDeleteAction {
  def run(args: List[String]): String = {
    assert(args.headOption.isDefined, "parameters must be provided")

    if(args.head != "_"){
      val yaml = args.head
      if (!yaml.isEmpty) {
        // to test the yaml string format
        Json_Yaml.yaml_to_object(yaml)
        HDFSOperatorV2.deleteDir(PathFun(ResourceActionHelper.getDir).add(s"${Md5.md5Hash(yaml)}.yml").toPath);
        return "success"
      }
    }

    assert(args.head == "_", "the first parameter must be _")
    val parser = new ParamsUtil(args.drop(1).mkString(" "))
    
    val t = parser.getParam("type")
    val path = parser.getParam("path")
    val allows = parser.getParam("allows", "")
    val denies = parser.getParam("denies", "")

    val authConfig = ResourceActionHelper.createAuthConfig(t, path, allows, denies)

    val content = Json_Yaml.object_to_yaml(authConfig)
    val dir = ResourceActionHelper.getDir
    HDFSOperatorV2.deleteDir(PathFun(dir).add(s"${Md5.md5Hash(content)}.yml").toPath);
    "success"
  }
}
