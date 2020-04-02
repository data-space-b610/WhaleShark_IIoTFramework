package ksb.csle.examples.energy.serving.pipe

import scala.util.parsing.json.JSONObject
import org.joda.time.DateTime
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{TimestampType, DateType}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import spray.json._
import ksb.csle.common.proto.DatasourceProto._

class AggregateDataKafka(kafka: KafkaInfo) extends Logging {
  
  val spark = SparkSession.builder()
     .config(new SparkConf())
     .master("local[*]")
     .getOrCreate()
     
  val producer: KafkaProducer[String,String] = createProducer(kafka)
  
  private def convertRow2Json(r: Row): String = {
    val m = r.schema.map(x => {
      x.dataType match {
        case t: TimestampType => (x.name -> "%s".format(r.getAs(x.name).toString))
        case d: DateType => (x.name -> "%s".format(r.getAs(x.name).toString))
        case _ => (x.name -> r.getAs(x.name))
      }
    }).toMap
    JSONObject(m).toString()
  }

  def createProducer(spec: KafkaInfo): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.getBootStrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }
  
  private def getDF(path: String): DataFrame = {
    spark.read.format("csv")
        .option("sep", ",")
        .option("header", true)
        .option("inferSchema", "true")
        .load(path)
        .cache
  }
  
  def writeTestData(path: String): Unit = {
    val d = getDF(path)
    d.show()
    d.collect.foreach{ row =>
      val key = "%s".format(new DateTime())
      val value = "%s".format(convertRow2Json(row))
      val record = new ProducerRecord[String, String](kafka.getTopic, key, value)
      logger.info("Send string message to Topic: " + kafka.getTopic +", "+ record)
      producer.send(record)
      Thread.sleep(10000)
//      Thread.sleep(scala.util.Random.nextInt(20 * 1000))
    }
                
    producer.close
  }
  
}

object AggregateDataKafka {
  private[this] val bootStrapServers: String = "csle1:9092"
  private[this] val zooKeeperConnect: String = "csle1:2181"
    
  def apply(o: KafkaInfo): AggregateDataKafka = new AggregateDataKafka(o)
  
  private def kafkaIngestion = {
    val kafka = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("ingestion-group")
      .setTopic("aggr-ingestion")
      .build

    val path = "hdfs://csle1:9000/datasets/energy/aggrEmulate.csv"
    AggregateDataKafka(kafka).writeTestData(path) 
  }
  
  def main(args: Array[String]) = {
    kafkaIngestion
  }
  
}
