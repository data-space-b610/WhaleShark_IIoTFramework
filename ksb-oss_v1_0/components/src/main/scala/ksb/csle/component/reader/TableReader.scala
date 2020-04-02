package ksb.csle.component.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.reader.BaseReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that loads records from the target table in SQL Database.
 * (e.g. PostgresQL, MySQL, ...).
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.TableInfo]]
 *          TableInfo contains attributes as follows:
 *          - jdbcUrl: JDBC URL of database to access (required)
 *          - jdbcDriver: JDBC driver of database to access (required)
 *          - tableName: Table name to access (required)
 *          - userName: User name for authentication (required)
 *          - password: Password for authentication (required)
 *
 * ==TableInfo==
 * {{{
 * message TableInfo {
 *   required string jdbcUrl = 1;
 *   required string jdbcDriver = 2;
 *   required string tableName = 3;
 *   required string userName = 4;
 *   required string password = 5;
 * }
 * }}}
 */
class TableReader(
    val o: BatchReaderInfo
    ) extends BaseReader[DataFrame, BatchReaderInfo, SparkSession](o) {

  private val info =
    if (o.getTableReader == null) {
      throw new IllegalArgumentException("TableInfo is not set.")
    } else {
      o.getTableReader
    }

  /**
   * Load records in as DataFrame from SQL Database.
   *
   * @param  session   Spark session
   * @return DataFrame DataFrame read from SQL Database
   */
  override def read(spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", info.getJdbcUrl)
      .option("dbtable", info.getTableName)
      .option("user", info.getUserName)
      .option("password", info.getPassword)
      .load()
  }

  /**
   * Close the TableReader.
   */
  override def close {
  }
}
