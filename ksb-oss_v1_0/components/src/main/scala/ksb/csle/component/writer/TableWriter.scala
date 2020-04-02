package ksb.csle.component.writer

import java.sql.{DriverManager, Connection, Statement}
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.utils.JdbcUtils

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that saves DataFrame to SQL Database.
 * (e.g. PostgresQL, MySQL, ...).
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.TableInfo]]
 *          TableInfo contains attributes as follows:
 *          - jdbcUrl: JDBC URL of database to save (required)
 *          - jdbcDriver: JDBC driver of database to save (required)
 *          - tableName: Table name to save (required)
 *          - userName: User name for authentication (required)
 *          - password: Password for authentication (required)
 *          - writeMode: Writing Mode; APPEND or OVERWRITE (optional)
 *          - primaryKey: List of column name to use as primary key (optional)
 *
 * ==TableInfo==
 * {{{
 * message TableInfo {
 *   required string jdbcUrl = 1;
 *   required string jdbcDriver = 2;
 *   required string tableName = 3;
 *   required string userName = 4;
 *   required string password = 5;
 *   optional WriteMode writeMode = 6 [default = APPEND];
 *   repeated string primaryKey = 7;
 * }
 * }}}
 */
class TableWriter(
    val o: BatchWriterInfo
    ) extends BaseWriter[DataFrame, BatchWriterInfo, SparkSession](o) {

  private val info =
    if (o.getTableWriter == null) {
      throw new IllegalArgumentException("TableInfo is not set.")
    } else {
      o.getTableWriter
    }

  private val conn = DriverManager.getConnection(info.getJdbcUrl,
      info.getUserName, info.getPassword)
  private val stmt = conn.createStatement()

  /**
   * Save DataFrame to the given table in SQL Database.
   *
   * @param df DataFrame to save
   */
  override def write(df: DataFrame) {
    logger.info(s"write DataFrame to '${info.getTableName}'")

    if (info.getWriteMode == WriteMode.OVERWRITE) {
      dropTable()
    }

    createTable(df)

    writeToTable(df)
  }

  private def dropTable() {
    val sql = s"DROP TABLE IF EXISTS ${info.getTableName}"
    logger.info("drop table: " + sql)

    stmt.executeUpdate(sql)
  }

  private def createTable(df: DataFrame) {
    val strSchema = JdbcUtils.schemaString(df.schema, info.getJdbcUrl)

    val sql =
      if (info.getPrimaryKeyList.isEmpty()) {
        "CREATE TABLE IF NOT EXISTS %s (%s)"
          .format(info.getTableName, strSchema)
      } else {
        "CREATE TABLE IF NOT EXISTS %s (%s CONSTRAINT pk PRIMARY KEY(%s))"
          .format(info.getTableName, strSchema,
              info.getPrimaryKeyList.toArray().mkString(","))
      }
    logger.info("create table: " + sql)

    stmt.executeUpdate(sql)
  }

  private def writeToTable(df: DataFrame) {
    val props = new Properties()
    props.put("driver", info.getJdbcDriver)
    props.put("user", info.getUserName)
    props.put("password", info.getPassword)

    df.write
      .mode(SaveMode.Append)
      .jdbc(info.getJdbcUrl, info.getTableName, props)
  }

  /**
   * Close the connection with SQL Database.
   */
  override def close: Any = {
    if (stmt != null) {
      stmt.close()
    }

    if (conn != null) {
      conn.close()
    }
  }
}
