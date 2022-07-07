package tech.mlsql.plugins.visualization

import net.csdn.common.settings.{ImmutableSettings, Settings}
import tech.mlsql.plugins.visualization.pylang.{Options, PyLang}

import scala.collection.JavaConverters._


//confFrom: confTable
//runtime:
//   env: {conda-env}
//fig:
//    line:
//       title: "日PV/UV曲线图"
//       x: day
//       y:
//        - pv
//        - uv
//       labels:
//           day: "日期"
//           pv: "pv"
//           uv: "uv"


class Fig(s: String) {
}

trait TranslateRuntime {
  def translate(s: String, dataset: String): String
}

case class VisualSource(confFrom: Option[String], runtime: Map[String, String], control: Settings, fig: Settings) {

  private def getFigType = {
    val key = fig.getAsMap.keySet().asList().get(0).split("\\.")(0)
    key
  }

  private def figParams = {
    fig.getByPrefix(getFigType + ".")
  }

  private def getFigParamsKeys = {
    figParams.getAsMap.keySet().asScala.map(item => item.split("\\.")(0)).toList
  }

  private def valueWithQuote(s: String) = {
    var realS = s
    var isStr = true
    try {
      s.toLong
      isStr = false
    } catch {
      case _: Exception =>
        try {
          s.toDouble
          isStr = false
        } catch {
          case _: Exception =>
            try {
              s.toBoolean
              isStr = false
              realS = s.capitalize
            } catch {
              case _: Exception =>
            }
        }
    }
    if (isStr) s""""${realS}"""" else realS
  }

  private def putIfPresentStringOrArray[T](s: String, k: Option[String], builder: Options[PyLang]): Option[String] = {
    val key = k.getOrElse(s)
    val x = figParams.get(s)

    if (x != null) {
      builder.addKV(key, valueWithQuote(x), None)
      return Some("")
    }
    val x1 = figParams.getAsArray(s)
    if (x1 != null && x1.length != 0) {
      val a = x1.map(item => s"""${valueWithQuote(item)}""").mkString(",")
      builder.addKV(key, s"[${a}]", None)
      return Some("")
    }
    return None
  }

  private def putIfPresentStringOrMap[T](s: String, k: Option[String], builder: Options[PyLang]): Option[String] = {
    val key = k.getOrElse(s)
    val x = figParams.get(s)
    if (x != null) {
      builder.addKV(key, valueWithQuote(x), None)
      return Some("")
    }
    val x1 = figParams.getByPrefix(s + ".")
    if (x1 != null) {
      val a = x1.getAsMap.asScala.toMap.map(item => s""""${item._1}":${valueWithQuote(item._2)}""").mkString(",")
      builder.addKV(key, s"{${a}}", None)
      return Some("")
    }
    return None
  }


  def toCode = {
    val pyLang = PyLang()

    // hints
    pyLang.raw.code("#%python").end
    if (!runtime.contains("schema")) {
      pyLang.raw.code("#%schema=st(field(content,string),field(mime,string))").end
    }
    runtime.foreach { item =>
      pyLang.raw.code(s"""#%${item._1}=${item._2}""").end
    }
    //default imports
    pyLang.raw.code("from pyjava.api.mlsql import RayContext,PythonContext").end
    pyLang.raw.code("from pyjava.api import Utils").end
    pyLang.raw.code("import plotly.express as px").end
    pyLang.raw.code("import base64").end

    //default variables
    pyLang.raw.code("context:PythonContext = context").end
    pyLang.raw.code("ray_context = RayContext.connect(globals(),None)").end
    pyLang.raw.code("df = ray_context.to_pandas()").end

    if (!control.getAsBoolean("ignoreSort", false)) {
      val builderTemp = pyLang.let("df").invokeFunc("sort_values").params
      putIfPresentStringOrArray[PyLang]("x", Some("by"), builderTemp)
      builderTemp.end.namedVariableName("df").end
    }

    val builder = pyLang.let("px").invokeFunc(getFigType).params.add("df", None)

    getFigParamsKeys.foreach { item =>
      val temp = putIfPresentStringOrArray[PyLang](item, None, builder)
      if (temp.isEmpty) {
        putIfPresentStringOrMap[PyLang](item, None, builder)
      }
    }

    builder.end.namedVariableName("fig").end
    val format = control.get("format", "html")
    format match {
      case "html" =>
        pyLang.raw.code("content = fig.to_html()").end
      case "image" =>
        pyLang.raw.code("""content = base64.b64encode(fig.to_image(format="png")).decode()""").end
    }
    pyLang.raw.code(s"""context.build_result([{"content":content,"mime":"${format}"}])""").end
    pyLang.toScript
  }

}


class PlotlyRuntime extends TranslateRuntime {
  private var rawVisualSource: Settings = null

  private def toVisualSource(s: Settings, dataset: String) = {
    val confFrom = s.get("confFrom")
    val runtimeConf = s.getByPrefix("runtime.")
    val controlConf = s.getByPrefix("control.")

    val fig = s.getByPrefix("fig.")
    val confFromTemp = if (confFrom != null) Some(confFrom) else None
    val runtimeConfTemp = if (runtimeConf != null) {
      runtimeConf.getAsMap.entrySet().asScala.map { kv =>
        (kv.getKey, kv.getValue)
      }.toMap
    } else Map[String, String]()

    val vs = VisualSource(confFromTemp, runtimeConfTemp ++ Map("input" -> dataset), controlConf, fig)
    vs
  }

  override def translate(s: String, dataset: String): String = {
    rawVisualSource = ImmutableSettings.settingsBuilder.loadFromSource(s).build()
    val vs = toVisualSource(rawVisualSource, dataset)
    vs.toCode
  }
}