package tech.mlsql.plugins.auth.simple.app

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class UserRule(resources: Resource, verbs: List[String])


case class UserRuleWrapper(rule: UserRule)

case class UserBlock(metadata: Metadata, rules: List[UserRuleWrapper])


case class ResourceRule(users: User, verbs: List[String])


case class ResourceRuleWrapper(rule: ResourceRule)

case class ResourceBlock(metadata: Metadata, rules: List[ResourceRuleWrapper])

case class User(allows: List[UserUnit], denies: List[UserUnit])

case class UserUnit(name: String, role: String)

case class Resource(allows: List[ResourceUnit], denies: List[ResourceUnit])

case class ResourceUnit(name: String, path: String)

case class Metadata(users: Option[List[UserUnit]], resources: Option[List[ResourceUnit]])

case class AuthConfig(
                       apiVersion: String,
                       kind: String,
                       userView: List[UserBlock],
                       resourceView: List[ResourceBlock]
                     )

object Json_Yaml {

  def object_to_json(authConfig: AuthConfig): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(authConfig)
  }

  def json_to_object(json: String): AuthConfig = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(json, classOf[AuthConfig])
  }

  def object_to_yaml(authConfig: AuthConfig): String = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(authConfig)
  }

  def yaml_to_object(yaml: String): AuthConfig = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(yaml, classOf[AuthConfig])
  }
}


