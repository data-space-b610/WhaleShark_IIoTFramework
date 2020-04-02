package ksb.csle.component.runner

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

class KafkaTest {
  val kafkaParams: scala.collection.immutable.Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  val conf: org.apache.spark.SparkConf = new SparkConf()
  val ssc: org.apache.spark.streaming.StreamingContext = new StreamingContext(conf, Seconds(1))
  val topics: Array[String] = Array("topicA", "topicB")
  val stream: org.apache.spark.streaming.dstream.InputDStream[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))

  stream.map(record => (record.key, record.value))
}
