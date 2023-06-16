package tech.mlsql.plugins.llm

import org.apache.spark.sql.UDFRegistration
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * 6/16/23 WilliamZhu(allwefantasy@gmail.com)
 */
object LLMUDF {
  def llm_stack(uDFRegistration: UDFRegistration) = {
    // llm_stack(chat(array(...)))
    uDFRegistration.register("llm_stack", (calls: Seq[String],newParams:Seq[String]) => {

      val obj = JSONTool.jParseJsonArray(calls.head).getJSONObject(0)
      val predict = obj.getString("predict")
      val input = obj.getJSONObject("input")
      val instruction = input.getString("instruction")
      // system_role,user_role,assistant_role
      val systemRole = input.optString("system_role", "")
      var userRole = input.optString("user_role", "")
      var assistantRole = input.optString("assistant_role", "")

      if (userRole != "") {
        userRole = s"${userRole}:"
      }

      if (assistantRole != "") {
        assistantRole = s"${assistantRole}:"
      }

      val his_instruction = s"${userRole}${instruction}\n${assistantRole}${predict}"
      val query = JSONTool.jParseJsonObj(newParams.head)
      query.put("instruction",s"${his_instruction}\n${query.getString("instruction")}")

      if(!query.has("system_role") && systemRole != ""){
        query.put("system_role",systemRole)
      }

      if (!query.has("user_role") && userRole != "") {
        query.put("user_role", userRole)
      }

      if (!query.has("assistant_role") && assistantRole != "") {
        query.put("assistant_role", assistantRole)
      }
      
      Seq(query.toString)
    })
  }

//  def llm_param(uDFRegistration: UDFRegistration) = {
//     session.
//  }
}
