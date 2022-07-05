package tech.mlsql.plugins.visiualization.test

import org.scalatest.FlatSpec
import tech.mlsql.plugins.visualization.{PlotlyRuntime, PythonHint}

/**
 * 2/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class FigTest extends FlatSpec {

  val template =
    """confFrom: confTable
      |runtime:
      |   env: "{conda-env}"
      |fig:
      |    line:
      |       title: "日PV/UV曲线图"
      |       x: day
      |       y:
      |        - pv
      |        - uv
      |       labels:
      |           day: "日期"
      |           pv: "pv"
      |           uv: "uv"""".stripMargin
  

  "yaml" should "to python code" in {
    val pr = new PlotlyRuntime()
    val pythonCode = pr.translate(template,"ww")
    val hint = new PythonHint()
    val byzerCode = hint.rewrite(pythonCode, Map())
    println(byzerCode)
  }
}
