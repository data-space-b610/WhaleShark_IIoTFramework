package ksb.csle.component.pipe.stream.writer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.pipe.writer.BasePipeWriter

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that writes datafrom to kafka.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.KafkaPipeWriterInfo]]
 *          KafkaPipeWriterInfo contains attributes as follows:
 *          - mode: Write mode such as append, update, and complete
 *          - trigger: triggering interval
 *          - bootStrapServers: Address of Kafka server (required)
 *          - zooKeeperConnect: Address of Kafka Zookeeper (required)
 *          - topic: Topic where fetch data (required)
 *          - chechpointLocation: checkpoint file path
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
 * ==KafkaPipeWriterInfo==
 * {{{
 *  message KafkaPipeReaderInfo {
 *    required string mode = 1 [default="append"];
 *    optional string trigger = 2;
 *    required string bootStrapServers = 3;
 *    required string zooKeeperConnect = 4;
 *    required string topic = 5;
 *    required chechpointLocation: checkpoint file path
 *    required bool failOnDataLoss = 7 [default = false];
 *  }
 * }}}
 * @param session: Sparksession
 */
class KafkaPipeWriter(
    val o: StreamPipeWriterInfo,
    val session: SparkSession
    ) extends BasePipeWriter[
      DataFrame, StreamingQuery, StreamPipeWriterInfo, SparkSession](o, session) {

  import session.implicits._
  val p: KafkaPipeWriterInfo = o.getKafkaPipeWriter

  override def write(df: DataFrame): StreamingQuery = {
    df
    .select(to_json(struct("*")) as "value")
    .writeStream
    .outputMode(p.getMode)
    .format("kafka")
    .option("kafka.bootstrap.servers", p.getBootStrapServers)
    .option("zookeeper.connect", p.getZooKeeperConnect)
    .option("topic", p.getTopic)
    .option("checkpointLocation", p.getCheckpointLocation)
    .option("failOnDataLoss", p.getFailOnDataLoss)
    .trigger(Trigger.ProcessingTime(p.getTrigger))
    .start()
  }

  override def close: Unit = ()
}
