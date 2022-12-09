package tech.mlsql.plugins.visiualization.test
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.plugins.visualization.{PlotlyRuntime, PythonHint}

/**
 * 2/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class FigTest  extends AnyFunSuite {

  def printCode(v: String) = {
    val pr = new PlotlyRuntime()
    val pythonCode = pr.translate(v, "ww")
    val hint = new PythonHint()
    val byzerCode = hint.rewrite(pythonCode, Map())
    println(byzerCode)
  }

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


  test("yaml should to python code") {
    printCode(template)
  }

  test("yaml should parse Boolean") {

    printCode(
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
        |      size_max: 60 """.stripMargin)
  }

  test("yaml should with vvtype") {
    printCode(
      """
        |confFrom: counties
        |runtime:
        |   env: source /opt/miniconda3/bin/activate ray-1.12.0
        |   cache: false
        |control:
        |   ignoreSort: True
        |fig:
        |   choropleth_mapbox:
        |      geojson:
        |         vv_type: jsonObj
        |         vv_value: counties
        |      locations: fips
        |      color: unemp
        |      color_continuous_scale: "Viridis"
        |      range_color: country
        |      mapbox_style: "carto-positron"
        |      zoom: 3
        |      center:
        |         lat: 37.0902
        |         lon: -95.7129
        |      opacity: 0.5
        |      labels:
        |         unemp: "失业率"""".stripMargin)
  }

  test("yaml should with matrix") {
    printCode(
      """
        |runtime:
        |   env: source /opt/miniconda3/bin/activate ray-1.12.0
        |   cache: false
        |control:
        |   ignoreSort: True
        |fig:
        |   matrix:
        |      annot: True
        |      cmap: "Blues"
        |      fmt: "d"
        |      cbar: False""".stripMargin)
  }

  test("yaml should with auc") {

    printCode(
      """
        |runtime:
        |   env: source /opt/miniconda3/bin/activate ray-1.12.0
        |   cache: false
        |control:
        |   ignoreSort: True
        |fig:
        |   auc:
        |     linewidth: 2
        |      """.stripMargin)
  }
}
