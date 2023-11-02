package tech.mlsql.plugins.execsql


import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister

import tech.mlsql.version.VersionCompatibility



/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
class ExecSQLApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {

    ETRegister.register("JDBCConn", classOf[JDBCConn].getName)
    CommandCollection.refreshCommandMapping(Map("conn" ->
      """
        |run command as JDBCConn.`` where parameters='''{:all}'''
        |""".stripMargin))

    ETRegister.register("JDBCExec", classOf[JDBCExec].getName)
    CommandCollection.refreshCommandMapping(Map("exec_sql" ->
      """
        |run command as JDBCExec.`` where parameters='''{:all}'''
        |""".stripMargin))
  }

  override def supportedVersions: Seq[String] = {
    ExecSQLApp.versions
  }

}

object ExecSQLApp extends Logging {
  val versions = Seq(">=2.0.1")

}
