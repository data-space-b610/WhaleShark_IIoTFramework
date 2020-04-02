package ksb.csle.component.writer

import java.util.Properties

import scala.util.parsing.json.JSONObject

import org.joda.time.DateTime

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Row}
import org.apache.spark.streaming.{ Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.base.writer.BaseWriter

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that sends DataFrame to Kafka.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.KafkaInfo]]
 *          KafkaInfo contains attributes as follows:
 *          - bootStrapServers: Address of Kafka server (required)
 *          - zooKeeperConnect: Address of Kafka Zookeeper (required)
 *          - groupId: Group ID of Kafka producers (required)
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
class KafkaWriter(
    val o: StreamWriterInfo
    ) extends BaseWriter[DataFrame, StreamWriterInfo, StreamingContext](o) {

  val p: KafkaInfo = o.getKafkaWriter
  val producer: KafkaProducer[String,String] = createProducer(p)

  /**
   * Send DataFrame to the given Kafka topic as JSON format.
   *
   * @param d DataFrame to send
   */
  override def write(d: DataFrame): Unit = {
    // <--
    d.toJSON.collect().foreach { json =>
      val record = new ProducerRecord[String, String](
          p.getTopic, DateTime.now().toString(), json)
      producer.send(record)
    }
    //
//    d.collect.foreach{ row =>
//      val key = "%s".format(new DateTime())
//      val value = "%s".format(convertRow2Json(row))
//      val record = new ProducerRecord[String, String](p.getTopic, key, value)
//      producer.send(record)
//    }
    // -->
    logger.info("Send string message to Topic: " + p.getTopic)
    logger.info("input dataframe row size : " + d.rdd.collect.size.toString)
  }

  /**
   * Close the connection with Kafka.
   */
  override def close: Unit = producer.close()

  private def convertRow2Json(r: Row): String = {
      val m = r.getValuesMap(r.schema.fieldNames)
      JSONObject(m).toString()
  }

  private def createProducer(
      spec: KafkaInfo): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        spec.getBootStrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
    // for large data.
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
        (1024 * 1024 * 1024).toString())
    new KafkaProducer[String, String](props)
  }
}

object KafkaWriter {
  def apply(o: StreamWriterInfo): KafkaWriter = new KafkaWriter(o)
}
