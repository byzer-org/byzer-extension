package tech.mlsql.plugins.visiualization.test

import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.plugins.visualization.pylang.PyLang

/**
 * 3/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class PyLangTest extends AnyFunSuite {
  test("assign should work") {
    val pl = PyLang().let("v").bind.str("jack").end.end
    assert(pl.toScript == """v = "jack"""")
  }

  test("variable should invoke func") {
    val pl = PyLang().let("ray_context").invokeFunc("to_pandas").end.end
    assert(pl.toScript == """ray_context.to_pandas()""")
  }

  test("variable should invoke func with new assignment") {
    val pl = PyLang().let("ray_context").invokeFunc("to_pandas").end.
      namedVariableName("df").end
    assert(pl.toScript == """df = ray_context.to_pandas()""")
  }

  test("variable should invoke object create with new assignment") {
    val pl = PyLang().let("ray_context").
      invokeObjectCreate.addImport("from pyjava.api.mlsql import RayContext,PythonContext").
      create("PythonContext").extend("object").
      end.namedVariableName("context").end
    println(pl.toScript)
    assert(pl.toScript ==
      """from pyjava.api.mlsql import RayContext,PythonContext
        |
        |
        |context = PythonContext()""".stripMargin)
  }

  test("right ident") {
    val genCode = PyLang().let("rayContext").invokeFunc("to_pandas").end.
      namedVariableName("df").end.
      func("add").
      block().let("a").bind.str("s").end.end.
      toScript
    assert(genCode ==
      """df = rayContext.to_pandas()
        |def add():
        |    a = "s"""".stripMargin)
  }

  test("with parameters") {
    val genCode = PyLang().let("rayContext").invokeFunc("to_pandas").params.
      add("day", None).addKV("y", "jack", Some("\"")).
      end.
      namedVariableName("df").end.
      toScript
    assert(genCode == "df = rayContext.to_pandas(day , y=\"jack\")")
  }
}
