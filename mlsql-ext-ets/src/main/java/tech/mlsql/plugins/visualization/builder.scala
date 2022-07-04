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

case class VisualSource(confFrom: Option[String], runtime: Map[String, String], fig: Settings) {

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

  private def putIfPresentStringOrArray[T](s: String, builder: Options[PyLang]): Option[String] = {
    val x = figParams.get(s)
    if (x != null) {
      builder.addKV(s, x, Some("\""))
      return Some("")
    }
    val x1 = figParams.getAsArray(s)
    if (x1 != null && x1.length != 0) {
      val a = x1.map(item => s""""${item}"""").mkString(",")
      builder.addKV(s, s"[${a}]", None)
      return Some("")
    }
    return None
  }

  private def putIfPresentStringOrMap[T](s: String, builder: Options[PyLang]): Option[String] = {
    val x = figParams.get(s)
    if (x != null) {
      builder.addKV(s, x, Some("\""))
      return Some("")
    }
    val x1 = figParams.getByPrefix(s + ".")
    if (x1 != null) {
      val a = x1.getAsMap.asScala.toMap.map(item => s"""${item._1}:"${item._2}"""").mkString(",")
      builder.addKV(s, s"{${a}}", None)
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

    //default variables
    pyLang.raw.code("context:PythonContext = context").end
    pyLang.raw.code("ray_context = RayContext.connect(globals(),None)").end
    pyLang.raw.code("df = ray_context.to_pandas()").end

    val builder = pyLang.let("fig").invokeFunc(getFigType).params.add("df", None)

    getFigParamsKeys.foreach { item =>
      val temp = putIfPresentStringOrArray[PyLang](item, builder)
      if (temp.isEmpty) {
        putIfPresentStringOrMap[PyLang](item, builder)
      }
    }

    builder.end.end

    pyLang.raw.code("content = fig.to_html()").end
    pyLang.raw.code("""context.build_result([{"content":content,"mime":"html"}])""").end
    pyLang.toScript
  }

}


class PlotlyRuntime extends TranslateRuntime {
  private var rawVisualSource: Settings = null

  private def toVisualSource(s: Settings, dataset: String) = {
    val confFrom = s.get("confFrom")
    val runtimeConf = s.getByPrefix("runtime.")
    val fig = s.getByPrefix("fig.")
    val confFromTemp = if (confFrom != null) Some(confFrom) else None
    val runtimeConfTemp = if (runtimeConf != null) {
      runtimeConf.getAsMap.entrySet().asScala.map { kv =>
        (kv.getKey, kv.getValue)
      }.toMap
    } else Map[String, String]()

    val vs = VisualSource(confFromTemp, runtimeConfTemp ++ Map("input" -> dataset), fig)
    vs
  }

  override def translate(s: String, dataset: String): String = {
    rawVisualSource = ImmutableSettings.settingsBuilder.loadFromSource(s).build()
    val vs = toVisualSource(rawVisualSource, dataset)
    vs.toCode
  }
}