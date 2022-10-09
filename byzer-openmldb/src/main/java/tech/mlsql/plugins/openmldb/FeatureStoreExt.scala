package tech.mlsql.plugins.openmldb

import com._4paradigm.openmldb.jdbc.SQLResultSet
import com._4paradigm.openmldb.sdk.SdkOption
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.{DataType, Schema => RsSchema}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.version.VersionCompatibility

import java.sql.{ResultSet, Statement}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class RsColumn(name: String, sqlType: DataType)

object FeatureStoreExt {
  def getRsCloumns(rs: ResultSet, schema: RsSchema): Array[RsColumn] = {
    (0 until schema.GetColumnCnt()).map { index =>
      RsColumn(schema.GetColumnName(index), schema.GetColumnType(index))
    }.toArray
  }

  def rsToMaps(rs: ResultSet): Seq[Map[String, Any]] = {
    val buffer = new ArrayBuffer[Map[String, Any]]()
    val rsI = rs.asInstanceOf[SQLResultSet]
    val innerSchema = rsI.GetInternalSchema()
    while (rs.next()) {
      buffer += rsToMap(rs, getRsCloumns(rs, innerSchema))
    }
    buffer
  }

  def rsToMap(rs: ResultSet, columns: Array[RsColumn]): Map[String, Any] = {
    val item = new mutable.HashMap[String, Any]()
    columns.zipWithIndex.foreach { case (col, _index) =>
      val index = _index + 1
      val v = col.sqlType match {
        // scalastyle:off
        case DataType.kTypeDate => rs.getDate(index)
        case DataType.kTypeInt64 => rs.getLong(index)
        case DataType.kTypeInt32 => rs.getInt(index)
        case DataType.kTypeBool => rs.getBoolean(index)
        case DataType.kTypeFloat => rs.getFloat(index)
        case DataType.kTypeTimestamp => rs.getTimestamp(index)
        case DataType.kTypeInt16 => rs.getInt(index)
        case DataType.kTypeString => rs.getString(index)
        case _ =>
          throw new java.sql.SQLException("Unrecognized SQL type " + col.name)
        // scalastyle:on

      }
      item.put(col.name, v)
    }
    item.toMap
  }

  def main(args: Array[String]): Unit = {
    val _zkAddress = "192.168.3.14:7181"
    val _zkPath = "/openmldb"
    val _code2 =
      """
        |LOAD DATA INFILE '/home/williamzhu/byzer-home/allwefantasy/sample_data/data/taxi_tour_table_train_simple'
        |INTO TABLE t1 options(format='parquet', header=true, mode='append');
        |""".stripMargin


    val _code =
      """
        |select * from t1 limit 10;
        |""".stripMargin


    val _action = "query"
    val _db = "demo_db"
    var sqlExecutor: SqlClusterExecutor = null
    var state: Statement = null

    try {
      val option = new SdkOption
      option.setZkCluster(_zkAddress)
      option.setZkPath(_zkPath)
      option.setSessionTimeout(1000000)
      option.setRequestTimeout(600000000)
      sqlExecutor = new SqlClusterExecutor(option)

      _action match {
        case "ddl" =>
          state = sqlExecutor.getStatement
          state.execute(s"use ${_db};")
          state.execute(s"SET @@sync_job=true;")
          state.execute("SET @@execute_mode='offline';")
          state.execute("SET @@job_timeout=20000000;")
          state.execute(_code)
        case _ =>
          state = sqlExecutor.getStatement
          state.execute(s"use ${_db}")
          val inputSchema = sqlExecutor.getInputSchema(_db, _code)
          state.execute(_code)
          val rs = state.getResultSet
          val resultSet = rsToMaps(rs)
          println(resultSet)
      }

    } finally {
      if (state != null) {
        state.close()
      }
      if (sqlExecutor != null) {
        sqlExecutor.close()
      }
    }
  }
}

class FeatureStoreExt(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  def getRsCloumns(rs: ResultSet, schema: RsSchema): Array[RsColumn] = {

    (0 until schema.GetColumnCnt()).map { index =>
      RsColumn(schema.GetColumnName(index), schema.GetColumnType(index))
    }.toArray
  }

  def rsToMaps(rs: ResultSet): Seq[Map[String, Any]] = {
    val buffer = new ArrayBuffer[Map[String, Any]]()
    val rsI = rs.asInstanceOf[SQLResultSet]
    val innerSchema = rsI.GetInternalSchema()
    while (rs.next()) {
      buffer += rsToMap(rs, getRsCloumns(rs, innerSchema))
    }
    buffer
  }

  def rsToMap(rs: ResultSet, columns: Array[RsColumn]): Map[String, Any] = {
    val item = new mutable.HashMap[String, Any]()
    columns.zipWithIndex.foreach { case (col, _index) =>
      val index = _index + 1
      val v = col.sqlType match {
        // scalastyle:off
        case DataType.kTypeDate => rs.getDate(index)
        case DataType.kTypeInt64 => rs.getLong(index)
        case DataType.kTypeInt32 => rs.getInt(index)
        case DataType.kTypeBool => rs.getBoolean(index)
        case DataType.kTypeFloat => rs.getFloat(index)
        case DataType.kTypeTimestamp => rs.getTimestamp(index)
        case DataType.kTypeInt16 => rs.getInt(index)
        case DataType.kTypeString => rs.getString(index)
        case _ =>
          throw new java.sql.SQLException("Unrecognized SQL type " + col.name)
        // scalastyle:on

      }
      item.put(col.name, v)
    }
    item.toMap
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val _zkAddress = params.getOrElse(zkAddress.name, "")
    val _zkPath = params.getOrElse(zkPath.name, "/openmldb")

    val statements = params.filter(f => """sql\-[0-9]+""".r.findFirstMatchIn(f._1).nonEmpty).
      map(f => (f._1.split("-").last.toInt, f._2)).toSeq.sortBy(f => f._1).map(f => f._2).map { f =>
      logInfo(s"${getClass.getName} execute: ${f}")
      f
    }

    val _action = params(action.name)
    val _db = params(db.name)
    var sqlExecutor: SqlClusterExecutor = null
    var state: Statement = null

    try {
      val option = new SdkOption
      
      if (_zkAddress.isEmpty) {
        option.setClusterMode(false)
        option.setHost(params.getOrElse("host", "127.0.0.0"))
        option.setPort(params.getOrElse("port", "6527").toInt)
      } else {
        option.setClusterMode(true)
        option.setZkCluster(_zkAddress)
        option.setZkPath(_zkPath)
      }

      option.setSessionTimeout(100000000)
      option.setRequestTimeout(600000000)
      sqlExecutor = new SqlClusterExecutor(option)

      state = sqlExecutor.getStatement
      state.execute(s"use ${_db}")
      var last: Boolean = false
      statements.foreach { code =>
        last = state.execute(code)
      }
      if (last) {
        val rs = state.getResultSet
        val resultSet = rsToMaps(rs)
        val rdd = df.sparkSession.sparkContext.parallelize(resultSet.map(item => JSONTool.toJsonStr(item)))
        return df.sparkSession.read.json(rdd)
      }


    } finally {
      if (state != null) {
        state.close()
      }
      if (sqlExecutor != null) {
        sqlExecutor.close()
      }
    }
    df.sparkSession.emptyDataFrame
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  final val db: Param[String] = new Param[String](this, "db",
    FormParams.toJson(Text(
      name = "db",
      value = "",
      extra = Extra(
        doc =
          """
            | The database name of OpenMLDB.
          """,
        label = "The database name of OpenMLDB.",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val action: Param[String] = new Param[String](this, "action",
    FormParams.toJson(Text(
      name = "action",
      value = "",
      extra = Extra(
        doc =
          """
            | The action of OpenMLDB. query|ddl
          """,
        label = "The action of OpenMLDB. query|ddl",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val zkAddress: Param[String] = new Param[String](this, "zkAddress",
    FormParams.toJson(Text(
      name = "zkAddress",
      value = "",
      extra = Extra(
        doc =
          """
            | zkAddress of the OpenMLDB
          """,
        label = "The zkAddress of the OpenMLDB",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val zkPath: Param[String] = new Param[String](this, "zkPath",
    FormParams.toJson(Text(
      name = "zkPath",
      value = "",
      extra = Extra(
        doc =
          """
            | zkPath of the OpenMLDB
          """,
        label = "The zkPath of the OpenMLDB",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = {
    Seq(">=1.6.0")
  }
}
