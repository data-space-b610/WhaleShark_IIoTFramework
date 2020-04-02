package ksb.csle.examples.device

import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

class HueConsumer {

  def get(): KafkaConsumer[String, String] = {
    val  props = new Properties()
    props.put("bootstrap.servers", "mymac:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    new KafkaConsumer[String, String](props)
  }
//  consumer.subscribe(util.Collections.singletonList(TOPIC))
//
//  while(true){
//    val records=consumer.poll(100)
//    for (record<-records.asScala){
//     println(record)
//    }
//  }
}
