//package ksb.csle.component.pipe.query.operator
//
//import scala.concurrent.Await
//import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration._
//import scala.collection.JavaConversions._
//import scala.collection.JavaConverters._
//import java.util.Properties
//
//import org.apache.logging.log4j.scala.Logging
//import org.apache.kafka.clients.consumer.KafkaConsumer
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import akka.actor.{ Actor, ActorLogging, ActorSystem }
//import spray.json._
//
//import ksb.csle.common.base.param.DeviceCtxCtlCase
//import ksb.csle.common.base.param.MyJsonProtocol._
//import ksb.csle.common.base.pipe.query.parser.BaseParser
//import ksb.csle.common.base.pipe.query.operator.BaseContextQueryPipeOperator
//import ksb.csle.common.proto.OndemandControlProto.OnDemandPipeOperatorInfo
//import ksb.csle.common.proto.OndemandControlProto.KafkaQueryPipeOperatorInfo
//import ksb.csle.common.base.pipe.query.parser.ContextParser
//import ksb.csle.component.pipe.query.operator.KafkaContextQueryPipeOperator._
//
///**
// * :: Experimental ::
// *
// * Operator that queries to kafka when it is requested
// */
//final class KafkaContextQueryPipeOperator(
//    override val p: OnDemandPipeOperatorInfo,
//    override implicit val system: ActorSystem
//    ) extends BaseContextQueryPipeOperator[DeviceCtxCtlCase, ActorSystem](p, system) with Logging {
//
//  private val info = p.getKafkaQueryOperator
//  private val props: Properties = createConsumerConfig(info)
//  private val consumer: KafkaConsumer[String,String] =
//    new KafkaConsumer[String, String](props)
//  private val timeout = 30
//  consumer.subscribe(java.util.Collections.singletonList(info.getTopic))
//
//  override def query(context: DeviceCtxCtlCase ): DeviceCtxCtlCase = {
//    val results = consumer.poll(timeout).asScala.map(_.value())
//    if (results.isEmpty) context
//    else {
//      val ctx = results.last.parseJson.convertTo[DeviceCtxCtlCase]
//      // FIXME
//      context.update(context, ctx)
//    }
//  }
//
//  override def stop = consumer.close()
//}
//
//object KafkaContextQueryPipeOperator {
//  def apply[P](
//      p: OnDemandPipeOperatorInfo,
//      system: ActorSystem) = new KafkaContextQueryPipeOperator(p, system)
//
//  def createConsumerConfig(p: KafkaQueryPipeOperatorInfo): Properties = {
//    val props = new Properties()
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getBootStrapServers)
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, p.getGroupId)
//    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
//    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//    props
//  }
//}
