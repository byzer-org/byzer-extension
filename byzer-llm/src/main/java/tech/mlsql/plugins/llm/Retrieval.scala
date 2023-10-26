package tech.mlsql.plugins.llm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.ets.Ray
import tech.mlsql.version.VersionCompatibility


/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 * run command as Retrieval.``
 * where action="cluster/create"
 * and `cluster_settings.name`=""
 * and `cluster_settings.location`="";
 * ;
 */

case class ClusterSettings(name: String, location: String, numNodes: Int)

object ClusterSettings {

  private val PREFIX = "cluster_settings"

  def fromMap(params: Map[String, String]) = {
    val name = params.getOrElse(s"${PREFIX}.name", "")
    val location = params.getOrElse(s"${PREFIX}.location", "")
    val numNodes = params.getOrElse(s"${PREFIX}.numNodes", "1").toInt
    new ClusterSettings(name, location, numNodes)
  }
}

case class EnvSettings(javaHome: String, path: String)

object EnvSettings {
  private val PREFIX = "env_settings"

  def fromMap(params: Map[String, String]) = {
    val javaHome = params.getOrElse(s"${PREFIX}.javaHome", "")
    val path = params.getOrElse(s"${PREFIX}.path", "")
    new EnvSettings(javaHome, path)
  }
}

case class JVMSettings(options: List[String])

object JVMSettings {
  private val PREFIX = "jvm_settings"

  def fromMap(params: Map[String, String]) = {

    val options = if (params.contains(s"${PREFIX}.options")) {
      params(s"${PREFIX}.options").split(",").toList
    } else {
      List()
    }
    new JVMSettings(options)
  }
}

case class ResourceRequirement(name: String, resourceQuantity: Float) {
}

case class ResourceRequirementSettings(resourceRequirements: List[ResourceRequirement])

object ResourceRequirementSettings {
  private val PREFIX = "resource_requirement_settings.resource_requirement."

  def fromMap(params: Map[String, String]) = {
    val resourceRequirements = params.filter(s => s._1.startsWith(s"${PREFIX}")).map(s => {
      val name = s._1.replace(s"${PREFIX}", "")
      val resourceQuantity = s._2.toFloat
      ResourceRequirement(name, resourceQuantity)
    }).toList
    new ResourceRequirementSettings(resourceRequirements)
  }
}


case class TableSettings(database: String, table: String, schema: String, location: String, num_shards: Int)

object TableSettings {
  private val PREFIX = "table_settings"

  def fromMap(params: Map[String, String]) = {
    val database = params.getOrElse(s"${PREFIX}.database", "")
    val table = params.getOrElse(s"${PREFIX}.table", "")
    val schema = params.getOrElse(s"${PREFIX}.schema", "")
    val location = params.getOrElse(s"${PREFIX}.location", "")
    val num_shards = params.getOrElse(s"${PREFIX}.num_shards", "1").toInt
    new TableSettings(database, table, schema, location, num_shards)
  }
}


class Retrieval(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val action = params.getOrElse("action", "table/search")

    action match {
      case "cluster/create" =>
        val clusterSettings = ClusterSettings.fromMap(params)
        val jvmSettings = JVMSettings.fromMap(params)
        val jvmOptionsStr = jvmSettings.options.map(item => s""" "${item}" """).mkString(",")
        val resourceRequirementSettings = ResourceRequirementSettings.fromMap(params)
        val resourceRequirementsStr = resourceRequirementSettings.resourceRequirements.map { item =>
          s"""
             |ResourceRequirement(
             |name="${item.name}",
             |resourceQuantity=${item.resourceQuantity}
             |)
             |""".stripMargin
        }.mkString(",")
        val session = ScriptSQLExec.context().execListener.sparkSession
        val code =
          s"""
             |import ray
             |import json
             |from pyjava import RayContext
             |from byzerllm.utils.retrieval import ByzerRetrieval
             |from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings,ResourceRequirement,ResourceRequirementSettings
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |
             |
             |byzer = ByzerRetrieval()
             |byzer.launch_gateway()
             |
             |v = byzer.start_cluster(
             |        cluster_settings=ClusterSettings(
             |            name="${clusterSettings.name}",
             |            location="${clusterSettings.location}",
             |            numNodes=${clusterSettings.numNodes}
             |        ),
             |            env_settings=EnvSettings(
             |            javaHome=context.conf["JAVA_HOME"],
             |            path=context.conf["PATH"]
             |        ),
             |            jvm_settings=JVMSettings(
             |            options=[${jvmOptionsStr}]
             |        ), resource_requirement_settings= ResourceRequirementSettings([${resourceRequirementsStr}])
             |        )
             |
             |ray_context.build_result([{"value":json.dumps({"status": v })}])
             |""".stripMargin
        logInfo(code)
        val trainer = new Ray()
        trainer.train(session.emptyDataFrame, "", Map(
          "code" -> code,
          "inputTable" -> "command",
          "outputTable" -> "output",
          "modelTable" -> "command"
        ))
      case "table/create" =>
        val clusterName = params("clusterName")
        val tableSettings = TableSettings.fromMap(params)
        val session = ScriptSQLExec.context().execListener.sparkSession
        val code =
          s"""
             |import ray
             |import json
             |from pyjava import RayContext
             |from byzerllm.utils.retrieval import ByzerRetrieval
             |from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings,ResourceRequirement,ResourceRequirementSettings
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |
             |
             |byzer = ByzerRetrieval()
             |byzer.launch_gateway()
             |
             |v = byzer.create_table("${clusterName}",
             |        TableSettings(
             |            database="${tableSettings.database}",
             |            table="${tableSettings.table}",
             |            schema='''${tableSettings.schema}''',
             |            location="${tableSettings.location}",
             |            num_shards=${tableSettings.num_shards}
             |        )
             |        )
             |
             |ray_context.build_result([{"value":json.dumps({"status": v })}])
             |""".stripMargin
        logInfo(code)
        val trainer = new Ray()
        trainer.train(session.emptyDataFrame, "", Map(
          "code" -> code,
          "inputTable" -> "command",
          "outputTable" -> "output",
          "modelTable" -> "command"
        ))
      case "table/data" =>
        val clusterName = params("clusterName")
        val database = params("database")
        val table = params("table")
        val batchSize = params.getOrElse("batchSize", "1000")
        val inputTable = params("inputTable")
        val session = ScriptSQLExec.context().execListener.sparkSession
        val code =
          s"""
             |import ray
             |import json
             |import numpy
             |from pyjava import RayContext
             |from byzerllm.utils.retrieval import ByzerRetrieval
             |from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |
             |
             |def handle_rows(rows):
             |    byzer = ByzerRetrieval()
             |    byzer.launch_gateway()
             |
             |    batch_size = ${batchSize}
             |
             |    data_refs = []
             |    for item in rows:
             |        for k,v in item.items():
             |            if isinstance(v, numpy.ndarray):
             |                item[k] = v.tolist()
             |        itemref = ray.put(json.dumps(item,ensure_ascii=False))
             |        data_refs.append(itemref)
             |        if len(data_refs) == batch_size:
             |            byzer.build("${clusterName}","${database}","${table}",data_refs)
             |            data_refs = []
             |
             |    if data_refs:
             |        byzer.build("${clusterName}","${database}","${table}",data_refs)
             |        data_refs = []
             |
             |    return [{"value":"true"}]
             |
             |ray_context.map_iter(handle_rows)
             |""".stripMargin
        logInfo(code)
        val trainer = new Ray()
        trainer.train(session.emptyDataFrame, "", Map(
          "code" -> code,
          "inputTable" -> inputTable,
          "outputTable" -> "output",
          "modelTable" -> "command"
        )).collect()
        session.emptyDataFrame

      case "table/commit" =>
        val clusterName = params("clusterName")
        val database = params("database")
        val table = params("table")
        val session = ScriptSQLExec.context().execListener.sparkSession
        val code =
          s"""
             |import ray
             |import json
             |from pyjava import RayContext
             |from byzerllm.utils.retrieval import ByzerRetrieval
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |
             |
             |byzer = ByzerRetrieval()
             |byzer.launch_gateway()
             |
             |v = byzer.commit("${clusterName}","${database}","${table}")
             |
             |ray_context.build_result([{"value":json.dumps({"status": v })}])
             |""".stripMargin
        logInfo(code)
        val trainer = new Ray()
        trainer.train(session.emptyDataFrame, "", Map(
          "code" -> code,
          "inputTable" -> "command",
          "outputTable" -> "output",
          "modelTable" -> "command"
        ))

      case "table/closeAndDeleteFile" =>
        val clusterName = params("clusterName")
        val database = params("database")
        val table = params("table")
        val session = ScriptSQLExec.context().execListener.sparkSession
        val code =
          s"""
             |import ray
             |import json
             |from pyjava import RayContext
             |from byzerllm.utils.retrieval import ByzerRetrieval
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |
             |
             |byzer = ByzerRetrieval()
             |byzer.launch_gateway()
             |
             |v = byzer.closeAndDeleteFile("${clusterName}","${database}","${table}")
             |
             |ray_context.build_result([{"value":json.dumps({"status": v })}])
             |""".stripMargin
        logInfo(code)
        val trainer = new Ray()
        trainer.train(session.emptyDataFrame, "", Map(
          "code" -> code,
          "inputTable" -> "command",
          "outputTable" -> "output",
          "modelTable" -> "command"
        ))

      case "table/truncate" =>
        val clusterName = params("clusterName")
        val database = params("database")
        val table = params("table")
        val session = ScriptSQLExec.context().execListener.sparkSession
        val code =
          s"""
             |import ray
             |import json
             |from pyjava import RayContext
             |from byzerllm.utils.retrieval import ByzerRetrieval
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |
             |
             |byzer = ByzerRetrieval()
             |byzer.launch_gateway()
             |
             |v = byzer.truncate("${clusterName}","${database}","${table}")
             |
             |ray_context.build_result([{"value":json.dumps({"status": v })}])
             |""".stripMargin
        logInfo(code)
        val trainer = new Ray()
        trainer.train(session.emptyDataFrame, "", Map(
          "code" -> code,
          "inputTable" -> "command",
          "outputTable" -> "output",
          "modelTable" -> "command"
        ))


      case "register" =>
        val session = ScriptSQLExec.context().execListener.sparkSession
        val udfName = params("udfName")
        val trainer = new Ray()
        val code =
          s"""
             |import ray
             |import numpy as np
             |from pyjava import RayContext,PythonContext
             |
             |from typing import Any, NoReturn, Callable, Dict, List
             |from byzerllm.utils.retrieval.udf import init_retrieval_client,search_func
             |
             |ray_context = RayContext.connect(globals(),context.conf["rayAddress"],job_config=ray.job_config.JobConfig(code_search_path=[context.conf["code_search_path"]],runtime_env={"env_vars": {"JAVA_HOME":context.conf["JAVA_HOME"],"PATH":context.conf["PATH"]}}))
             |byzer=init_retrieval_client([],context.conf)
             |input_value = [row["value"] for row in ray_context.python_context.fetch_once_as_rows()]
             |v = search_func(byzer,input_value)
             |context.build_result([v])
             |""".stripMargin
        logInfo(code)
        trainer.predict(session, "command", udfName, Map(
          "predictCode" -> code,
          "reconnect" -> "true",
          "sourceSchema" -> "st(field(value,string))",
          "outputSchema" -> "st(field(value,array(string)))"
        ) ++ params)
        session.emptyDataFrame

      case _ =>
        throw new RuntimeException(s"action ${action} is not supported")
    }
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = {
    Seq(">=1.6.0")
  }

}
