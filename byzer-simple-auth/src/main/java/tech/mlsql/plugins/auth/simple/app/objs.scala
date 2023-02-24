package tech.mlsql.plugins.auth.simple.app

case class UserRule(resources: Resource, verbs: List[String])


case class UserBlock(metadata: Metadata, rules: List[UserRule])


case class ResourceRule(users: User, verbs: List[String])


case class ResourceBlock(metadata: Metadata, rules: List[ResourceRule])

case class User(allows: List[ResourceUnit], denies: List[ResourceUnit])

case class UserUnit(name: String, role: String)

case class Resource(allows: List[UserUnit], denies: List[UserUnit])

case class ResourceUnit(name: String, path: String)

case class Metadata(user: Option[List[UserUnit]], resources: Option[List[ResourceUnit]])

case class AuthConfig(
                       apiVersion: String,
                       kind: String,
                       userView: List[UserBlock],
                       resourceView: List[ResourceBlock]
                     )


