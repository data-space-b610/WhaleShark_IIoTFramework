package ksb.csle.component.pipe.stream.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamReader

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.StreamPipeControlProto.GroupbyPipeInfo
import ksb.csle.common.base.pipe.reader.BasePipeReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that reads data from kafka and pipelines it to the next.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.KafkaPipeReaderInfo]]
 *          KafkaPipeReaderInfo contains attributes as follows:
 *          - bootStrapServers: Address of Kafka server (required)
 *          - zooKeeperConnect: Address of Kafka Zookeeper (required)
 *          - topic: Topic where fetch data (required)
 *          - addTimestamp: Flag for adding timestamp column automatically
 *          - timestampName: Column name for timestamp field
 *          - watermark: Time slot in seconds or minutest
 *                       The event time column and the threshold on
 *                       how late the data is expected to be
 *                       in terms of event time.
 *          - sampleJsonPath: Json file path containing sample dataset.
 *                            The system gets hint for the record format from it.
 *          - failOnDataLoss: Determines whether or not a streaming query
 *                            should fail if it's possible data has been lost
 *                            (e.g., topics are deleted, offsets are out of range).
 *                            It is important to monitor your streaming queries,
 *                            especially with temporal infrastructure like Kafka.
 *                            Offsets typically go out of range
 *                            when Kafka's log cleaner activates.
 *                            If a specific streaming query can not process
 *                            data quickly enough it may fall behind
 *                            the earliest offsets after the log cleaner rolls
 *                            a log segment
 *                            Sometimes failOnDataLoss may be a false alarm.
 *                            You can disable it if it is not working
 *                            as expected based on your use case.
 *                            Refer to followed site for more information.
 *                            https://github.com/vertica/PSTL/wiki/Kafka-Source
 *
 * ==KafkaPipeReaderInfo==
 * {{{
 *  message KafkaPipeReaderInfo {
 *    required string bootStrapServers = 1;
 *    required string zooKeeperConnect = 2;
 *    required string topic = 3;
 *    // FIXME: Avoid the case that the addTimestamp is 'true' with no setting of timestampName"
 *    required bool addTimestamp = 4 [default = true];
 *    optional string timestampName = 5;
 *    optional string watermark = 6;
 *    optional string sampleJsonPath = 7;
 *    required bool failOnDataLoss = 8 [default = false];
 *  }
 * }}}
 * @param session: Sparksession
 */
class KafkaPipeReader(
    val o: StreamPipeReaderInfo,
    val session: SparkSession
    ) extends BasePipeReader[DataFrame, StreamPipeReaderInfo, SparkSession](o, session) {

  import session.implicits._

  val p: KafkaPipeReaderInfo = o.getKafkaPipeReader
  val topic: String = p.getTopic
  val df: DataStreamReader = session
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", p.getBootStrapServers)
    .option("zookeeper.connect", p.getZooKeeperConnect)
    .option("subscribe", p.getTopic)
    .option("failOnDataLoss", p.getFailOnDataLoss)

  val schema =
    if (p.hasSampleJsonPath)
      session.read.json(p.getSampleJsonPath).schema
    else {
      // FIXME: Get schema from parameter o.
      new StructType()
      .add("sensorId", "string")
      .add("sensorType", "integer")
      .add("event", "string")
      .add("timestamp", "timestamp")
    }

  /**
   * Reads data from kafka topic
   * TODO: Define and use parameters to set watermark and timestamp column.
   *
   * @return dataframe
   */
  override def read: DataFrame = {
    // FIXME: Do not add timestamp if data contains it.

    var result = df.load()
      .select($"value" cast "string" as "json")
      .select(from_json($"json", schema) as "data")
      .select("data.*")

    if (p.getAddTimestamp)
      result = result.withColumn(p.getTimestampName, current_timestamp())
    // json normally considers the timestamp type as the string type.
    // Note that the column to be declared for the watermark should be the timestamp type
    if (p.hasTimestampName) {
      val ts = to_timestamp(result.col(p.getTimestampName), "yyyy-MM-dd HH:mm:ss")
      result = result.withColumn(p.getTimestampName, ts)
    }
    if (p.hasWatermark)
      result = result.withWatermark(p.getTimestampName, p.getWatermark)

    logger.info(result.printSchema().toString())

    (result)
  }

  override def close: Unit = ()
}
