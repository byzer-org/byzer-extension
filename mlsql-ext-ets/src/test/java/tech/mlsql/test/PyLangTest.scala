package tech.mlsql.test

import org.scalatest.FlatSpec
import tech.mlsql.plugins.visualization.pylang.PyLang

/**
 * 3/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class PyLangTest extends FlatSpec {
  "assign" should "work" in {
    val pl = PyLang().let("v").bind.str("jack").end.end
    assert(pl.toScript == """v = "jack"""")
  }

  "variable" should "invoke func" in {
    val pl = PyLang().let("ray_context").invokeFunc("to_pandas").end.end
    assert(pl.toScript == """ray_context.to_pandas()""")
  }

  "variable" should "invoke func with new assignment" in {
    val pl = PyLang().let("ray_context").invokeFunc("to_pandas").end.
      namedVariableName("df").end
    assert(pl.toScript == """df = ray_context.to_pandas()""")
  }

  "variable" should "invoke object create with new assignment" in {
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

  "func" should "right ident" in {
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

  "variable" should "with parameters" in {
    val genCode = PyLang().let("rayContext").invokeFunc("to_pandas").params.
      add("day", None).addKV("y", "jack", Some("\"")).
      end.
      namedVariableName("df").end.
      toScript
    assert(genCode == "df = rayContext.to_pandas(day , y = \"jack\")")
  }
}
