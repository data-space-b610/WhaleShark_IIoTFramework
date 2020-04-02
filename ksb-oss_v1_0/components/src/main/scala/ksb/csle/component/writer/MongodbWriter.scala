package ksb.csle.component.writer

import org.apache.spark.sql._

import com.mongodb.spark._
import com.mongodb.spark.config._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.writer.BaseWriter

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that saves DataFrame to MongoDB.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.MongodbInfo]]
 *          MongodbInfo contains attributes as follows:
 *          - serverAddress: MongoDB address
 *                           (e.g. 192.168.0.3:27017) (required)
 *          - dbName: Database name to save (required)
 *          - collectionName: Collection name to save (required)
 *          - userName: User name if requires authentication (optional)
 *          - password: Password if requires authentication (optional)
 *
 * ==MongodbInfo==
 * {{{
 * message MongodbInfo {
 *   required string serverAddress = 1;
 *   required string dbName = 2;
 *   required string collectionName = 3;
 *   optional string userName = 4;
 *   optional string password = 5;
 * }
 * }}}
 */
class MongodbWriter(
    val o: BatchWriterInfo
    ) extends BaseWriter[DataFrame, BatchWriterInfo, SparkSession](o) {

  private val info =
    if (o.getMongodbWriter == null) {
      throw new IllegalArgumentException("MongodbInfo is not set.")
    } else {
      o.getMongodbWriter
    }

  /**
   * Save DataFrame to the given MongoDB collection.
   *
   * @param df DataFrame to save
   */
  override def write(df: DataFrame) {
    logger.info(s"write DataFrame to '${info.getCollectionName}'")

    val writeConfig = WriteConfig(Map(
        "uri" -> s"mongodb://${info.getServerAddress}",
        "database" -> info.getDbName,
        "collection" -> info.getCollectionName))

    MongoSpark.save(df, writeConfig)
  }

  /**
   * Close the MongodbWriter.
   */
  override def close: Unit = {
  }
}
