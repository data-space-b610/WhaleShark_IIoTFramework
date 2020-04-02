package ksb.csle.component.writer

import java.sql.{DriverManager, Connection, Statement}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.writer.BaseWriter

import PhoenixWriter._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that saves DataFrame to HBase using Phoenix.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.PhoenixInfo]]
 *          PhoenixInfo contains attributes as follows:
 *          - jdbcUrl: JDBC URL of Phoenix (required)
 *          - zkUrl: Address of HBase Zookeeper (required)
 *          - tableName: Table name to save (required)
 *          - userName: User name if requires authentication (optional)
 *          - password: Password if requires authentication (optional)
 *          - writeMode: Writing Mode; APPEND or OVERWRITE (optional)
 *          - primaryKey: List of column name to use as primary key (optional)
 *
 * ==PhoenixInfo==
 * {{{
 * message PhoenixInfo {
 *   required string jdbcUrl = 1;
 *   required string zkUrl = 2;
 *   required string tableName = 3;
 *   optional string userName = 4;
 *   optional string password = 5;
 *   optional WriteMode writeMode = 6 [default = APPEND];
 *   repeated string primaryKey = 7;
 * }
 * }}}
 */
class PhoenixWriter(
    val o: BatchWriterInfo
    ) extends BaseWriter[DataFrame, BatchWriterInfo, SparkSession](o) {

  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")

  private val info =
    if (o.getPhoenixWriter == null) {
      throw new IllegalArgumentException("PhoenixInfo is not set.")
    } else {
      o.getPhoenixWriter
    }

  private val conn = DriverManager.getConnection(info.getJdbcUrl)
  private val stmt = conn.createStatement()

 /**
   * Save DataFrame to the given HBase table using Phoenix.
   *
   * @param df DataFrame to save
   */
  override def write(df: DataFrame) {
    logger.info(s"write DataFrame to '${info.getTableName}'")

    if (info.getWriteMode == WriteMode.OVERWRITE) {
      dropTable()
    }

    val (dfWithPkCols, pks) = addPrimaryKeys(df)

    createColumnFamilyTable(dfWithPkCols, pks)
    writeToColumnFamilyTable(dfWithPkCols, pks)
  }

  private def dropTable() {
    val sql = s"DROP TABLE IF EXISTS ${info.getTableName}"
    logger.info("drop table: " + sql)

    stmt.executeUpdate(sql)
  }

  private def addPrimaryKeys(df: DataFrame): (DataFrame, Array[String]) = {
    var dfWithPkCols: DataFrame = df
    var pks: Array[String] = null

    if (info.getPrimaryKeyList.isEmpty()) {
      dfWithPkCols = df
        .withColumn(PK_WRITE_TIME, current_timestamp())
        .withColumn(PK_ROW_ID, monotonically_increasing_id())
      pks = Array[String](PK_WRITE_TIME, PK_ROW_ID)
    } else {
      pks = info.getPrimaryKeyList.toList.toArray
    }

    (dfWithPkCols, pks)
  }

  private def createColumnFamilyTable(df: DataFrame, pks: Array[String]) = {
    val columnInfos = df.schema.map(col => {
      if (col.nullable)
        if(pks contains col.name.toUpperCase.trim) // pk should be not null
          "%s %s not null".format(col.name.trim, toPhoenxDataType(col.dataType))
        else "\"%s\".%s %s".format(
          col.name.trim.substring(0,3), col.name.trim, toPhoenxDataType(col.dataType))
      else
        if(pks contains col.name.toUpperCase.trim)
          "%s %s not null".format(col.name.trim, toPhoenxDataType(col.dataType))
        else "\"%s\".%s %s not null".format(
          col.name.trim.substring(0,3), col.name.trim, toPhoenxDataType(col.dataType))
    })

    createTableQuery(columnInfos.mkString(","),
        pks.map(x => "\"%s\"".format(x)).mkString(","))
  }

  private def createTableQuery(columnInfos: String, pks: String) {
    val sql = "CREATE TABLE IF NOT EXISTS %s (%s CONSTRAINT pk PRIMARY KEY(%s))"
      .format(info.getTableName, columnInfos.toUpperCase, pks.toUpperCase)
    logger.info("create table: " + sql)

    stmt.executeUpdate(sql)
  }

  private def toPhoenxDataType(dataType: DataType): String = dataType match {
    case BooleanType => "BOOLEAN"
    case DateType => "TIMESTAMP"
    case ByteType => "INTEGER"
    case DoubleType => "DOUBLE"
    case FloatType => "FLOAT"
    case IntegerType => "INTEGER"
    case LongType => "BIGINT"
    case ShortType => "INTEGER"
    case TimestampType => "TIME"
    case StringType => "VARCHAR"
    case _ => throw new RuntimeException("not convertable data type: " + dataType)
  }

  private def writeToTable(df: DataFrame) {
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite) // SaveMode other than SaveMode.OverWrite is not supported.
      .option("table", info.getTableName)
      .option("zkUrl", info.getZkUrl)
      .save()
  }

  private def writeToColumnFamilyTable(df: DataFrame, pks: Array[String]) {
    val columnInfos = df.schema.map(col =>
      if(pks contains col.name.trim) "%s".format(col.name.trim)
      else "%s.%s".format(col.name.trim.substring(0,3), col.name.trim))

    var index = 0
    df.rdd.collect.map(row => {
      if(index % 1000 == 0) conn.commit()
      val columnValues = row.schema.map(x =>
        x.dataType match {
          case n: NumericType => "%s".format(row.getAs(x.name).toString)
          case _ => "\'%s\'".format(row.getAs(x.name).toString)
        }
      ).mkString(",")

      upsertTableQuery(columnInfos.mkString(","), columnValues)
      index += 1
    })

    conn.commit()
  }

  private def upsertTableQuery(columnInfos: String, columnValues: String) = {
    val sql = "UPSERT INTO %s (%s) VALUES (%s)".format(
        info.getTableName, columnInfos, columnValues)
//    logger.info("upsert query: " + sql)

    stmt.executeUpdate(sql)
  }

  /**
   * Close the connection with HBase.
   */
  override def close = {
    if (stmt != null) {
      stmt.close()
    }

    if (conn != null) {
      conn.close()
    }
  }
}

object PhoenixWriter {
  val PK_WRITE_TIME = "pk_write_time"
  val PK_ROW_ID = "pk_row_id"

  def apply(o: BatchWriterInfo): PhoenixWriter = new PhoenixWriter(o)
}
