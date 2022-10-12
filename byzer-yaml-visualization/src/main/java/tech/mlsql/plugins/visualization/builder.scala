package tech.mlsql.plugins.visualization

import net.csdn.common.settings.{ImmutableSettings, Settings}
import org.apache.commons.lang3.StringUtils
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


object FigUtils {
  def valueWithQuote(s: String) = {
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
}

trait TranslateRuntime {
  def translate(s: String, dataset: String): String
}

case class VVValue(k: String, vvValue: String, vvType: Option[String]) {


  def toValue: String = {
    vvType match {
      case Some("number") => vvValue
      case Some("bool") => vvValue.capitalize
      case Some("code") => vvValue
      case Some("jsonObj") => s"""json.loads(context.conf["${vvValue}"])"""
      case Some("ref") => s"""context.conf["${vvValue}"]"""
      case None =>
        FigUtils.valueWithQuote(vvValue)
    }
  }
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

  private def tryGetValueWithVVType(settings: Settings, s: String): Option[VVValue] = {
    val x = settings.get(s)
    if (x == null) {
      val x1 = settings.getByPrefix(s + ".").getAsMap
      if (x1.get("vv_type") != null) {
        return Some(VVValue(s, x1.get("vv_value"), Some(x1.get("vv_type"))))
      } else {
        return None
      }
    }

    Some(VVValue(s, x, None))
  }

  private def putIfPresentStringOrArray[T](s: String, k: Option[String], builder: Options[PyLang]): Option[String] = {
    val key = k.getOrElse(s)
    val x = tryGetValueWithVVType(figParams, s)
    if (x.isDefined) {
      builder.addKV(key, x.get.toValue, None)
      return Some("")
    }
    val x1 = figParams.getAsArray(s)
    if (x1 != null && x1.length != 0) {
      val a = x1.map(item => s"""${FigUtils.valueWithQuote(item)}""").mkString(",")
      builder.addKV(key, s"[${a}]", None)
      return Some("")
    }
    return None
  }

  private def putIfPresentStringOrMap[T](s: String, k: Option[String], builder: Options[PyLang]): Option[String] = {
    val key = k.getOrElse(s)
    val x = tryGetValueWithVVType(figParams, s)
    if (x.isDefined) {
      builder.addKV(key, x.get.toValue, None)
      return Some("")
    }
    val x1 = figParams.getByPrefix(s + ".")
    if (x1 != null) {
      val a = x1.getAsMap.asScala.toMap.map(item => s""""${item._1}":${FigUtils.valueWithQuote(item._2)}""").mkString(",")
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

    if (confFrom.isDefined) {
      pyLang.raw.code(s"""#%confTable=${confFrom.get}""").end
    }

    var imageType = control.get("backend")
    if (StringUtils.isEmpty(imageType)) {
      getFigType match {
        case "matrix" | "auc" => imageType = "sklearn"
        case _ => imageType = "plotly"
      }
    }

    //default imports
    pyLang.raw.code("from pyjava.api.mlsql import RayContext,PythonContext").end
    pyLang.raw.code("from pyjava.api import Utils").end
    pyLang.raw.code("import base64").end
    pyLang.raw.code("import json").end

    //默认使用plotly
    imageType match {
      case "sklearn" => {
        pyLang.raw.code("import matplotlib.pyplot as plt").end
        pyLang.raw.code("import seaborn as sns").end
      }
      case _ => {
        pyLang.raw.code("import plotly.express as px").end
        pyLang.raw.code("import plotly.graph_objects as go")
      }
    }

    //default variables
    pyLang.raw.code("context:PythonContext = context").end
    pyLang.raw.code("ray_context = RayContext.connect(globals(),None)").end
    pyLang.raw.code("df = ray_context.to_pandas()").end

    if (!control.getAsBoolean("ignoreSort", false)) {
      val builderTemp = pyLang.let("df").invokeFunc("sort_values").params
      putIfPresentStringOrArray[PyLang]("x", Some("by"), builderTemp)
      builderTemp.end.namedVariableName("df").end
    }

    var builder: Options[PyLang] = null

    imageType match {
      case "sklearn" => {
        builder = getFigType match {
          case "matrix" =>
            pyLang.raw.code(
              """
                |from sklearn.metrics import confusion_matrix
                |cm = confusion_matrix(df['label'], df['prediction'])
                |plt.figure()
                |""".stripMargin).end
            pyLang.let("sns").invokeFunc("heatmap").params.add("cm", None)
          case "auc" =>
            pyLang.raw.code(
              """
                |import sklearn.metrics as metrics
                |fpr, tpr, threshold = metrics.roc_curve(df['label'], df['probability'])
                |roc_auc = metrics.auc(fpr, tpr)
                |plt.figure()
                |_, ax = plt.subplots()
                |""".stripMargin).end
            val auc = pyLang.let("ax").invokeFunc("plot").params
              .add("fpr", None)
              .add("tpr", None)
              .add("'b'", None)
              .addKV("label", "'AUC = %0.2f' % roc_auc", None)
            pyLang.raw.code(
              """
                |plt.legend(loc = 'lower right')
                |ax.plot([0, 1], [0, 1],'r--')
                |plt.xlim([0, 1])
                |plt.ylim([0, 1])
                |plt.ylabel('True Positive Rate')
                |plt.xlabel('False Positive Rate')
                |""".stripMargin).end
            auc
          case _ =>
            pyLang.let("plt").invokeFunc(getFigType).params.add("df", None)
        }
      }
      case _ => {
        builder = pyLang.let("px").invokeFunc(getFigType).params.add("df", None)
      }
    }


    getFigParamsKeys.foreach { item =>
      val temp = putIfPresentStringOrArray[PyLang](item, None, builder)
      if (temp.isEmpty) {
        putIfPresentStringOrMap[PyLang](item, None, builder)
      }
    }

    builder.end.namedVariableName("fig").end
    var format = control.get("format", "html")

    imageType match {
      case "sklearn" => {
        format = "image"
        pyLang.raw.code("content = Utils.gen_img(plt)").end
      }
      case _ => {
        format match {
          case "html" =>
            pyLang.raw.code("content = fig.to_html()").end
          case "image" =>
            pyLang.raw.code("""content = base64.b64encode(fig.to_image(format="png")).decode()""").end
          case _ => {
            format = "html"
            pyLang.raw.code("content = '<h1>format格式错误</h1>'").end
          }
        }
      }
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