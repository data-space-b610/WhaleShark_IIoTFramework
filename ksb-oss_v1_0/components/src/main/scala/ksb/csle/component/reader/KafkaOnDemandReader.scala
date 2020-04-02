package ksb.csle.component.reader

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.reader.BaseReader

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that fetchs data from Kafka.
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
class KafkaOnDemandReader(
    val o: OnDemandReaderInfo
    ) extends BaseReader[
      Long => Option[String], OnDemandReaderInfo, Long](o) {

  val kafkaReaderInfo: KafkaInfo = o.getKafkaOnDemandReader
  val props: java.util.Properties = KafkaOnDemandReader.createConsumerConfig(
      kafkaReaderInfo)
  val consumer: KafkaConsumer[String,String] =
    new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList(kafkaReaderInfo.getTopic))

  /**
   * Read a data from Kafka within a given time.
   *
   * @param  timeout        Read timeout
   * @return Option[String] A data
   */
  override def read(timeout: Long): Long => Option[String] = { timeout =>
    val results = consumer.poll(timeout).asScala.map(_.value())
    if (results.isEmpty) {
      None
    } else {
      Some(results.last)
    }
  }

  /**
   * Close the connection with Kafka.
   */
  override def close: Unit = {
    consumer.close()
  }
}

object KafkaOnDemandReader {
  def apply[P](o: OnDemandReaderInfo): KafkaOnDemandReader =
    new KafkaOnDemandReader(o)

  def createConsumerConfig(p: KafkaInfo): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getBootStrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, p.getGroupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }
}
