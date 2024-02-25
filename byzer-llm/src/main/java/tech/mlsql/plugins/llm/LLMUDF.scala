package tech.mlsql.plugins.llm

import org.apache.spark.sql.UDFRegistration
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.plugins.llm.utils.LLMUtils.content_split
import tech.mlsql.tool.Templates2

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * 6/16/23 WilliamZhu(allwefantasy@gmail.com)
 */
object LLMUDF {
  def llm_stack(uDFRegistration: UDFRegistration) = {
    // llm_stack(chat(array(...)))
    uDFRegistration.register("llm_stack", (calls: Seq[String], newParams: Seq[String]) => {

      val obj = JSONTool.jParseJsonObj(calls.head)
      val history = obj.getJSONArray("history")
      var query = new mutable.HashMap[String,String]()
      query += ("history" -> history.toString)
      val temp = JSONTool.jParseJsonObj(newParams.head)
      temp.keySet().asScala.foreach { key =>
        query += (key.toString -> temp.getString(key.toString))
      }
      Seq(JSONTool.toJsonStr(query.toMap))
    })
  }

  def llm_param(uDFRegistration: UDFRegistration) = {
    // select ins,
    //qianwen_chat(array(to_json(map("instruction",ins)))) as story
    //from table1 as table2;
    uDFRegistration.register("llm_param", (input: Map[String, String]) => {
      Seq(JSONTool.toJsonStr(input))
    })
  }

  def llm_result(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_result", (calls: Seq[String]) => {
      val obj = JSONTool.jParseJsonObj(calls.head)
      obj.getString("output")
    })
  }

  def llm_response_predict(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_response_predict", (calls: Seq[String]) => {
      val obj = JSONTool.jParseJsonObj(calls.head)
      obj.getString("output")
    })
  }

  // llm_prompt('hello {0}',array('world')
  def llm_prompt(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_prompt", (template: String, keys: Seq[String]) => {
      Templates2.evaluate(template, keys)
    })
  }

  /**
   *
   * @param content split content
   * @param splits split chars
   * @param limits max split length
   *
   */
  def llm_split(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_split", (content: String, splits: Seq[String], limits: Int) => {
      content_split(content, splits, limits)
    })
  }
}
