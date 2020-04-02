package ksb.csle.component.pipe.stream.reader

import java.util.Properties
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.logging.log4j.scala.Logging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import spray.json._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.param.DeviceCtxCtlCase
import ksb.csle.common.base.param.MyJsonProtocol._
import ksb.csle.common.base.pipe.query.parser.BaseParser
import ksb.csle.common.base.pipe.query.operator.BaseContextQueryPipeOperator
import ksb.csle.common.base.pipe.reader.BasePipeReader
import ksb.csle.component.pipe.stream.reader.KafkaContextQueryPipeReader._

/**
 * :: Experimental ::
 *
 * Reader that queries to kafka in on-demand way.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.KafkaContextQueryReaderInfo]]
 *          KafkaContextQueryReaderInfo contains attributes as follows:
 *          - bootStrapServers: Address of Kafka server (required)
 *          - zooKeeperConnect: Address of Kafka Zookeeper (required)
 *          - groupId: Group ID of Kafka consumers (required)
 *          - topic: Topic where fetch data (required)
 *          - contextParser: experimental (optional)
 *

 * ==KafkaContextQueryReaderInfo==
 * {{{
 *  message KafkaContextQueryReaderInfo {
 *    required string bootStrapServers = 1;
 *    required string zooKeeperConnect = 2;
 *    required string groupId = 3;
 *    required string topic = 4;
 *    optional ContextParserPair contextParser = 5;
 *  }
 * }}}
 *
 * ==ContextParserPair==
 *  message ContextParserPair {
 *    required string context = 1;
 *    required string parser = 2;
 *  }
 */
final class KafkaContextQueryPipeReader(
    val p: OnDemandPipeReaderInfo,
    implicit val system: ActorSystem
    ) extends BasePipeReader[DeviceCtxCtlCase, OnDemandPipeReaderInfo, ActorSystem](p, system) {

  private val info = p.getKafkaContextQueryReader
  private val props: Properties = createConsumerConfig(info)
  private val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String, String](props)
  private val timeout = 30
  consumer.subscribe(java.util.Collections.singletonList(info.getTopic))

  override def read: DeviceCtxCtlCase = {
    var results = Iterable.empty[String]
    while (results.isEmpty) {
      results = consumer.poll(timeout).asScala.map(_.value())
    }
    results.last.parseJson.convertTo[DeviceCtxCtlCase]
  }

  override def close = consumer.close()
}

object KafkaContextQueryPipeReader {
  def apply[P](
      p: OnDemandPipeReaderInfo,
      system: ActorSystem) = new KafkaContextQueryPipeReader(p, system)

  def createConsumerConfig(p: KafkaContextQueryReaderInfo): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getBootStrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, p.getGroupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }
}
