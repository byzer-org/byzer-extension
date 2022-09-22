package tech.mlsql.plugins.render

import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.SparkOperationUtil
import streaming.core.strategy.platform.PlatformManager
import streaming.dsl.ScriptSQLExec
import tech.mlsql.app.ExceptionRender
import tech.mlsql.common.utils.serder.json.JsonUtils
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.log.LogUtils
import tech.mlsql.plugins.render.ExceptionReplaceTemplate.getMsgTemplate

import java.io.File
import java.util.regex.Pattern
import scala.io.Source

class ExceptionReplaceRender extends ExceptionRender with Logging  {

  private val matchExp: ThreadLocal[Option[String]] = new ThreadLocal[Option[String]] {
    override def initialValue(): Option[String] = {
      Option.empty[String]
    }
  }

  def getMatchExp(): Option[String] = matchExp.get()

  override def format(e: Exception): String = {
    val matched = matchExp.get().get
    matchExp.set(Option.empty)

    val paramsJson = ScriptSQLExec.context().userDefinedParam("__PARAMS__")
    val showStack = JsonUtils.fromJson[Map[String, AnyRef]](paramsJson)
                             .getOrElse("show_stack", "false").asInstanceOf[String].toBoolean

    if( showStack ) matched + System.lineSeparator() + LogUtils.format_exception(e)
    else            matched
  }

  override def is_match(e: Exception): Boolean = {
    getMsgTemplate().exists { m =>

      if( m._1.matcher(e.getMessage).matches() ) {
        // Save matched error message
        matchExp.set( Some( m._2) )
        true
      }
      else {
        false
      }

    }
  }

}

object ExceptionReplaceTemplate extends SparkOperationUtil with Logging {
  val confFileName = "err-msg-template.json"

  private var inited: Boolean = false
  private var msgTemplate: Seq[(Pattern, String)] = Seq.empty

  def getMsgTemplate() : Seq[(Pattern, String)] = {
    synchronized {
      if (inited) return msgTemplate
      else {
        val sep = File.separator
        val paramsUtil = PlatformManager.getOrCreate.config.get()
        val confLocation = paramsUtil.getParam("streaming.render.byzerReplaceRender.config.location")
        val byzerHome = System.getenv("BYZER_HOME")

        if(StringUtils.isNotBlank(confLocation)) {
          logInfo(s"Reading template from ${confLocation}")
          msgTemplate = readJsonFile( s"${confLocation}${sep}${confFileName}")
        }
        else if(StringUtils.isNotBlank(byzerHome) && msgTemplate.isEmpty) {
          val f = s"${byzerHome}${sep}conf${sep}${confFileName}"
          logInfo(s"Reading template from ${f}")
          msgTemplate = readJsonFile( f )
        }
        else {
          logInfo(s"Reading template from classpath")
          val fullPath = Thread.currentThread().getContextClassLoader.getResource(s"${confFileName}").toString
          msgTemplate = readJsonFile(fullPath)
        }
        inited = true
        msgTemplate
      }
    }
  }

  def readJsonFile(fullName: String): Seq[(Pattern, String)] = {
    val file = new File(fullName )
    if( ! file.exists()) return Seq.empty

    val f = Source.fromFile( file )
    tryWithResource(f) { _ =>
      val jsonStr = f.mkString
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats

      parse(jsonStr).extract[Seq[RegExpAndMessage]]
        .map(r => Pattern.compile(r.regexp) -> r.msg)
    }
  }

}

case class RegExpAndMessage(regexp: String, msg: String)
