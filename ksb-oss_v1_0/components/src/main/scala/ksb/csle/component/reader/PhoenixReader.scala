package ksb.csle.component.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.reader.BaseReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that loads records from HBase using Phoenix.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.PhoenixInfo]]
 *          PhoenixInfo contains attributes as follows:
 *          - jdbcUrl: JDBC URL of Phoenix (required)
 *          - zkUrl: Address of HBase Zookeeper (required)
 *          - tableName: Table name to access (required)
 *          - userName: User name if requires authentication (optional)
 *          - password: Password if requires authentication (optional)
 *
 * ==PhoenixInfo==
 * {{{
 * message PhoenixInfo {
 *   required string jdbcUrl = 1;
 *   required string zkUrl = 2;
 *   required string tableName = 3;
 *   optional string userName = 4;
 *   optional string password = 5;
 * }
 * }}}
 */
class PhoenixReader(
    val o: BatchReaderInfo
    ) extends BaseReader[
      DataFrame, BatchReaderInfo, SparkSession](o) with HBaseFuncAPI {

  private val info =
    if (o.getPhoenixReader == null) {
      throw new IllegalArgumentException("PhoenixInfo is not set.")
    } else {
      o.getPhoenixReader
    }

  /**
   * Loads records in as DataFrame from HBase using Pheonix SQL.
   *
   * @param  session   Spark session
   * @return DataFrame DataFrame read from HBase
   */
  override def read(spark: SparkSession): DataFrame =
    read(spark, info)

 /**
   * Close the PhoenixReader.
   */
  override def close {
  }
}

object PhoenixReader {
  def apply[P](o: BatchReaderInfo): PhoenixReader = new PhoenixReader(o)
}
