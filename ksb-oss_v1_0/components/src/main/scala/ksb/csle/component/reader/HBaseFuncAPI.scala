package ksb.csle.component.reader

import org.apache.logging.log4j.scala.Logging
import java.sql.{Statement, ResultSet}
import java.util.{TimeZone, Calendar}
import scala.collection.JavaConversions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import ksb.csle.common.proto.DatasourceProto.PhoenixInfo

/**
 * An utility class to interact with HBase.
 */
trait HBaseFuncAPI extends Logging {

  def getTableSchema(stmt: Statement, tableName: String): StructType = {
    val meta = executeQuery(
        stmt, s"SELECT * FROM ${tableName} LIMIT 10").getMetaData
    val nColumns = meta.getColumnCount
    StructType(List.range(1, nColumns+1).map(x =>
      toDataFrameType(meta.getColumnName(x),
          meta.getColumnTypeName(x))).toSeq)
  }

  def read(spark: SparkSession, info: PhoenixInfo): DataFrame = {
    import spark.implicits._

    var df: DataFrame = null
    try {
      df = spark.sqlContext.load(
          "org.apache.phoenix.spark",
          Map("table" -> info.getTableName,
              "zkUrl" -> info.getZkUrl))
      logger.info(df.show.toString)
      logger.info(df.printSchema.toString)
      df
    } catch {
      case e: ClassCastException =>
        logger.error(s"Unsupported type cast error: ${e.getMessage}")
      case e: UnsupportedOperationException =>
        logger.error(s"Unsupported file reading error: ${e.getMessage}")
      case e: Throwable =>
        logger.error(s"Unknown file reading error: ${e.getMessage}")
    }
    df
  }

  def toDataFrameType(name: String, dataType: String): StructField = {
    dataType match {
      case "BOOLEAN" => new StructField(name, BooleanType)
      case "TIMESTAMP" => new StructField(name, TimestampType)
      case "INTEGER" => new StructField(name, IntegerType)
      case "DOUBLE" => new StructField(name, DoubleType)
      case "FLOAT" => new StructField(name, FloatType)
      case "BIGINT" => new StructField(name, LongType)
      case "VARCHAR" => new StructField(name, StringType)
    }
  }

  def getValueByType(rs: ResultSet, typeStr: String, index: Int): Any = {
    typeStr match {
      case "BOOLEAN" => rs.getBoolean(index)
      case "TIMESTAMP" => rs.getTimestamp(index,
          Calendar.getInstance(TimeZone.getTimeZone("UTC")))
      case "INTEGER" => rs.getInt(index)
      case "DOUBLE" => rs.getDouble(index)
      case "FLOAT" => rs.getFloat(index)
      case "BIGINT" => rs.getInt(index)
      case "VARCHAR" => rs.getString(index)
    }
  }

  def executeQuery(stmt: Statement, sql: String): ResultSet = {
    var rs: ResultSet = null
    try {
      logger.info("query: " + sql)
      rs = stmt.executeQuery(sql)
    } catch {
      case e: Throwable =>
        logger.error(s"Unknown file reading error: ${e.getMessage}")
    }
    (rs)
  }
}
