package ksb.csle.component.reader

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.reader.BaseReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that receives streaming data from Kafka.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.KafkaInfo]]
 *          KafkaInfo contains attributes as follows:
 *          - bootStrapServers: Address of Kafka server (required)
 *          - zooKeeperConnect: Address of Kafka Zookeeper (required)
 *          - groupId: Group ID of Kafka consumers (required)
 *          - topic: Topic where fetch data (required)
 *
 * ==KafkaInfo==
 * {{{
 * message KafkaInfo {
 *   required string bootStrapServers = 1;
 *   required string zooKeeperConnect = 2;
 *   required string groupId = 3;
 *   required string topic = 4;
 * }
 * }}}
 */
class KafkaReader(
    val o: StreamReaderInfo
    ) extends BaseReader[
      InputDStream[
        ConsumerRecord[String, String]], StreamReaderInfo,
        StreamingContext](o) {

  val p: KafkaInfo = o.getKafkaReader
  val kafkaParams: scala.collection.immutable.Map[String,Object] = Map[String, Object](
    "bootstrap.servers" -> p.getBootStrapServers,
    "zookeeper.connect" -> p.getZooKeeperConnect,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> p.getGroupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics: Array[String] = Array(p.getTopic)

  /**
   * Create an input stream to receives streaming data from Kafka.
   *
   * @param  ssc                                          Spark streaming context
   * @return InputDStream[ConsumerRecord[String, String]] Input stream to read streaming data
   */
  override def read(
      ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val stream : InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
    stream
  }

  /**
   * Close the KafkaReader.
   */
  override def close: Unit = ()
}

object KafkaReader {
  def apply[P](o: StreamReaderInfo): KafkaReader = new KafkaReader(o)
}
