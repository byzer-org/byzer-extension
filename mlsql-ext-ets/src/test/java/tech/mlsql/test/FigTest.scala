package tech.mlsql.test

import net.csdn.common.settings.ImmutableSettings
import org.scalatest.FlatSpec
import tech.mlsql.plugins.visualization.PlotlyRuntime

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

  "fig" should "from-yaml" in {
    val pr = new PlotlyRuntime()
    val rawVisualSource = ImmutableSettings.settingsBuilder.loadFromSource(template).build()
    val vs = pr.toVisualSource(rawVisualSource)
    val map = vs.fig.getAsMap
    val key = map.keySet().asList().get(0).split("\\.")(0)
    println(key)
    val lineFig = vs.fig.getByPrefix(key + ".")
    println(lineFig.getAsMap)
    println(lineFig.getAsArray("y").toList)
  }

  "yaml" should "to python code" in {
    val pr = new PlotlyRuntime()
    val pythonCode = pr.translate(template,"ww")
    println(pythonCode)
  }
}
