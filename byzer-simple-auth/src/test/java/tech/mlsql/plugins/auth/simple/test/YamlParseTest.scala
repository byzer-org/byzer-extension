package tech.mlsql.plugins.auth.simple.test

import org.scalatest.funsuite.AnyFunSuite
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, OperateType, TableAuthResult, TableTypeMeta}
import tech.mlsql.plugins.auth.simple.app.ByzerSimpleAuth

import java.io.File

/**
 * 26/2/2023 WilliamZhu(allwefantasy@gmail.com)
 */
class YamlParseTest extends AnyFunSuite {
  ByzerSimpleAuth.v_spark_mlsql_auth_simple_dir = Some(new File("./byzer-simple-auth/src/test/resources").getAbsolutePath)
  test("parse yaml file") {
    val auth = new ByzerSimpleAuth()
    val v = auth.auth(List(MLSQLTable(Option("mlsql_system"), Option("__auth_admin__"), OperateType.EMPTY, Option(""), TableTypeMeta("", Set()))))
    assert(v.size == 1, "should be 1")
    assert(v.head.granted == false, "testUser should be not allowed to access __auth_admin__")
  }

  test("test admin access __auth_admin__") {
    val newContext = ScriptSQLExec.contextGetOrForTest().copy(owner = "admin")
    ScriptSQLExec.setContext(newContext)

    val auth = new ByzerSimpleAuth()
    val v = auth.auth(List(MLSQLTable(Option("mlsql_system"), Option("__auth_admin__"), OperateType.SELECT, Option(""), TableTypeMeta("", Set()))))

    assert(v.head.granted == true, "admin should be able to access __auth_admin__")
  }

  test("test jack access s3a://bucket2/jack/tmp/jack") {
    val newContext = ScriptSQLExec.contextGetOrForTest().copy(owner = "jack")
    ScriptSQLExec.setContext(newContext)

    val auth = new ByzerSimpleAuth()
    val v = auth.auth(List(MLSQLTable(None, Option("s3a://bucket2/jack/tmp/jack"), OperateType.LOAD, Option(""), TableTypeMeta("", Set()))))

    assert(v.head.granted == true, "jack should be able to access s3a://bucket2/jack/tmp/jack")
  }

  test("test wow access s3a://bucket2/jack/tmp/jack") {
    val newContext = ScriptSQLExec.contextGetOrForTest().copy(owner = "wow")
    ScriptSQLExec.setContext(newContext)

    val auth = new ByzerSimpleAuth()
    val v = auth.auth(List(MLSQLTable(None, Option("s3a://bucket2/jack/tmp/jack"), OperateType.LOAD, Option(""), TableTypeMeta("", Set()))))

    assert(v.head.granted == false, "wow should be not allowed to access s3a://bucket2/jack/tmp/jack")
  }
}
