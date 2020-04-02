package ksb.csle.component.reader

import java.sql.DriverManager

import scala.collection.JavaConversions._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.reader.BaseReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that loads records from HBase using Phoenix SQL.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.PhoenixInfo]]
 *          PhoenixInfo contains attributes as follows:
 *          - jdbcUrl: JDBC URL of Phoenix (required)
 *          - zkUrl: Address of HBase Zookeeper (required)
 *          - tableName: Table name to access (required)
 *          - userName: User name if requires authentication (optional)
 *          - password: Password if requires authentication (optional)
 *          - sql: List of SQL to execute when loads records (optional)
 *
 * ==PhoenixInfo==
 * {{{
 * message PhoenixInfo {
 *   required string jdbcUrl = 1;
 *   required string zkUrl = 2;
 *   required string tableName = 3;
 *   optional string userName = 4;
 *   optional string password = 5;
 *   repeated string sql = 8;
 * }
 * }}}
 */
class PhoenixReaderBySql(
    val o: BatchReaderInfo
    ) extends BaseReader[
      DataFrame, BatchReaderInfo, SparkSession](o) with HBaseFuncAPI {

  private val info =
    if (o.getPhoenixReader == null) {
      throw new IllegalArgumentException("PhoenixInfo is not set.")
    } else {
      o.getPhoenixReader
    }

  val conn = DriverManager.getConnection(info.getJdbcUrl)
  val stmt = conn.createStatement()

  /**
   * This reads DataFrame from HBase table using Apache Phoenix.
   *
   * @param spark SparkSession.
   * @return DataFrame.
   */

  override def read(spark: SparkSession): DataFrame = {
    if(info.getSqlList.isEmpty) read(spark, info)
    else readDataBySql(spark)
  }

  private def readDataBySql(spark: SparkSession): DataFrame = {
    var result: DataFrame = null
    val schema = getTableSchema(stmt, info.getTableName)

    info.getSqlList.toList.map(sql => {
      val rs = executeQuery(stmt, sql)
      val nColumns = rs.getMetaData.getColumnCount
      val nColumnTypes = List.range(1, nColumns+1)
        .map(x => rs.getMetaData.getColumnTypeName(x)).toArray
      var rowList = new scala.collection.mutable.ListBuffer[Row]()
      var index = 1
      while(rs.next()) {
        val seq = List.range(1, nColumns+1).map(x =>
              getValueByType(rs, nColumnTypes(x-1), x)).toSeq
        rowList += Row.fromSeq(seq)
        if(index % 1000000 == 0) {
          val tmpDF = spark.createDataFrame(rowList.toList, schema)
          result = {
              if(result == null) tmpDF
              else result.unionAll(tmpDF)
            }
          rowList.clear()
          println(index + "-th records are generated")
          result.show
        }
        index += 1
      }
      val tmpDF = spark.createDataFrame(rowList.toList, schema)
      result = {
          if(result == null) tmpDF
          else result.unionAll(tmpDF)
        }
    })
    result.show()
    result
  }

  override def close {
  }

}

object PhoenixReaderBySql {
  def apply[P](o: BatchReaderInfo): PhoenixReaderBySql = new PhoenixReaderBySql(o)
}
