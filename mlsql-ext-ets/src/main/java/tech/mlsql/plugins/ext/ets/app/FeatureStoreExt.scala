package tech.mlsql.plugins.ext.ets.app

import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.sdk.{Column, Schema, SdkOption}
import net.sf.json.JSONObject
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.version.VersionCompatibility

import java.sql.{ResultSet, Statement}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 20/6/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class FeatureStoreExt(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  def getRsCloumns(rs: ResultSet, schema: Schema): Array[Column] = {
    val columnList = schema.getColumnList
    (0 until columnList.size()).map { index =>
      columnList.get(index)
    }.toArray
  }

  def rsToMaps(rs: ResultSet, schema: Schema): Seq[Map[String, Any]] = {
    val buffer = new ArrayBuffer[Map[String, Any]]()
    while (rs.next()) {
      buffer += rsToMap(rs, getRsCloumns(rs, schema))
    }
    buffer
  }

  def rsToMap(rs: ResultSet, columns: Array[Column]): Map[String, Any] = {
    val item = new mutable.HashMap[String, Any]()
    columns.zipWithIndex.foreach { case (col, _index) =>
      val index = _index + 1
      val v = col.getSqlType match {
        // scalastyle:off
        case java.sql.Types.ARRAY => null
        case java.sql.Types.BIGINT => rs.getLong(index)
        case java.sql.Types.BINARY => null
        case java.sql.Types.BIT => rs.getBoolean(index) // @see JdbcDialect for quirks
        case java.sql.Types.BLOB => null
        case java.sql.Types.BOOLEAN => rs.getBoolean(index)
        case java.sql.Types.CHAR => rs.getString(index)
        case java.sql.Types.CLOB => rs.getString(index)
        case java.sql.Types.DATALINK => null
        case java.sql.Types.DATE => rs.getDate(index)
        case java.sql.Types.DISTINCT => null
        case java.sql.Types.DOUBLE => rs.getDouble(index)
        case java.sql.Types.FLOAT => rs.getFloat(index)
        case java.sql.Types.INTEGER => rs.getInt(index)
        case java.sql.Types.JAVA_OBJECT => null
        case java.sql.Types.LONGNVARCHAR => rs.getString(index)
        case java.sql.Types.LONGVARBINARY => null
        case java.sql.Types.LONGVARCHAR => rs.getString(index)
        case java.sql.Types.NCHAR => rs.getString(index)
        case java.sql.Types.NCLOB => rs.getString(index)
        case java.sql.Types.NULL => null
        case java.sql.Types.NUMERIC => null
        case java.sql.Types.NVARCHAR => rs.getString(index)
        case java.sql.Types.OTHER => null
        case java.sql.Types.REAL => rs.getDouble(index)
        case java.sql.Types.REF => rs.getString(index)
        case java.sql.Types.REF_CURSOR => null
        case java.sql.Types.ROWID => rs.getLong(index)
        case java.sql.Types.SMALLINT => rs.getInt(index)
        case java.sql.Types.SQLXML => rs.getString(index)
        case java.sql.Types.STRUCT => rs.getString(index)
        case java.sql.Types.TIME => rs.getTimestamp(index)
        case java.sql.Types.TIME_WITH_TIMEZONE
        => null
        case java.sql.Types.TIMESTAMP => rs.getTimestamp(index)
        case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
        => null
        case java.sql.Types.TINYINT => rs.getInt(index)
        case java.sql.Types.VARBINARY => null
        case java.sql.Types.VARCHAR => rs.getString(index)
        case _ =>
          throw new java.sql.SQLException("Unrecognized SQL type " + col.getSqlType)
        // scalastyle:on

      }
      item.put(col.getColumnName, v)
    }
    item.toMap
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val _zkAddress = params(zkAddress.name)
    val _zkPath = params.getOrElse(zkPath.name, "/openmldb")
    val _code = params(code.name)
    val _action = params(action.name)
    val _db = params(db.name)
    var sqlExecutor: SqlClusterExecutor = null
    var state: Statement = null

    try {
      val option = new SdkOption
      option.setZkCluster(_zkAddress)
      option.setZkPath(_zkPath)
      option.setSessionTimeout(10000)
      option.setRequestTimeout(600000)
      sqlExecutor = new SqlClusterExecutor(option)

      _action match {
        case "ddl" =>
          state = sqlExecutor.getStatement
          state.execute(s"use ${_db}")
          state.execute(_code)
        case _ =>
          state = sqlExecutor.getStatement
          state.execute(s"use ${_db}")
          val inputSchema = sqlExecutor.getInputSchema(_db, _code)
          val rs = state.executeQuery(_code)
          val resultSet = rsToMaps(rs, inputSchema: Schema)
          val rdd = df.sparkSession.sparkContext.parallelize(resultSet.map(item => JSONObject.fromObject(item.asJava).toString()))
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

  final val code: Param[String] = new Param[String](this, "code",
    FormParams.toJson(Text(
      name = "code",
      value = "",
      extra = Extra(
        doc =
          """
            | The code of OpenMLDB
          """,
        label = "The code of OpenMLDB",
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
