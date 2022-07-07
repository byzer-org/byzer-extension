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
      |control:
      |   ignoreSort: false
      |   format: image
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

  "yaml" should "parse Boolean" in  {
     val t =
       """
         |runtime:
         |   env: source /opt/miniconda3/bin/activate ray-1.12.0
         |   cache: false
         |fig:
         |   scatter:
         |      title: "欧洲人口分布"
         |      x: gdpPercap
         |      y: lifeExp
         |      size: pop
         |      color: continent
         |      hover_name: country
         |      log_x: True
         |      size_max: 60 """.stripMargin

    val pr = new PlotlyRuntime()
    val pythonCode = pr.translate(t,"ww")
    val hint = new PythonHint()
    val byzerCode = hint.rewrite(pythonCode, Map())
    println(byzerCode)
  }
}
