package tech.mlsql.plugins.assert.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.assert.ets.{Assert, AssertCondition, AssertConditionThrow, AssertNotNull, AssertNotNullThrow, AssertUniqueKey, AssertUniqueKeyThrow, AssertUniqueKeys, AssertUniqueKeysThrow, MLSQLThrow}
import tech.mlsql.version.VersionCompatibility

/**
 * 4/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLAssert extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("Assert", classOf[Assert].getName)
    ETRegister.register("Throw", classOf[MLSQLThrow].getName)
    ETRegister.register("AssertNotNull", classOf[AssertNotNull].getName)
    ETRegister.register("AssertNotNullThrow", classOf[AssertNotNullThrow].getName)
    ETRegister.register("AssertUniqueKey", classOf[AssertUniqueKey].getName)
    ETRegister.register("AssertUniqueKeyThrow", classOf[AssertUniqueKeyThrow].getName)
    ETRegister.register("AssertUniqueKeys", classOf[AssertUniqueKeys].getName)
    ETRegister.register("AssertUniqueKeysThrow", classOf[AssertUniqueKeysThrow].getName)
    ETRegister.register("AssertCondition", classOf[AssertCondition].getName)
    ETRegister.register("AssertConditionThrow", classOf[AssertConditionThrow].getName)
    CommandCollection.refreshCommandMapping(Map("assertNotNull" ->
      """
        |run command as AssertNotNull.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertNotNullThrow" ->
      """
        |run command as AssertNotNullThrow.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertUniqueKey" ->
      """
        |run command as AssertUniqueKey.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertUniqueKeyThrow" ->
      """
        |run command as AssertUniqueKeyThrow.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertUniqueKeys" ->
      """
        |run command as AssertUniqueKeys.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertUniqueKeysThrow" ->
      """
        |run command as AssertUniqueKeysThrow.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertCondition" ->
      """
        |run command as AssertCondition.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assertConditionThrow" ->
      """
        |run command as AssertConditionThrow.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("assert" ->
      """
        |run command as Assert.`` where parameters='''{:all}'''
        |""".stripMargin))
    CommandCollection.refreshCommandMapping(Map("throw" ->
      """
        |run command as Throw.`` where msg='''{0}'''
        |""".stripMargin))
  }


  override def supportedVersions: Seq[String] = {
    MLSQLAssert.versions
  }
}

object MLSQLAssert {
  val versions = Seq("2.1.0", "2.1.0-SNAPSHOT", "2.0.0", "2.0.1")
}