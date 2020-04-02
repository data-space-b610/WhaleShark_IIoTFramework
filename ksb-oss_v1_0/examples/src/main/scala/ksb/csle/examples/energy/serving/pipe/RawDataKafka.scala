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

class RawDataKafka(kafka: KafkaInfo) extends Logging {

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

  def writeData(path: String): Unit = {
    val d = getDF(path)

    var index = 2;
//    val sensorList = Seq.range(54, 63) ++ Seq(24,52,64,67,70,73,76,79,82,85,88,91,93)
    val sensorList = Seq.range(59, 63) ++ Seq(24,52,54,56,64,93)
    sensorList.map(sensorId => {
      val df = d.filter(d("TOPIC_ID") === sensorId)

      new Thread(new DataSend(df, sensorId)).start()
    })

  }

  class DataSend(df: DataFrame, sensorId: Int) extends Runnable {
    def run() {
      df.collect.foreach{ row =>
        val key = "%s".format(new DateTime())
        val value = "%s".format(convertRow2Json(row))
        val record = new ProducerRecord[String, String](s"topic_$sensorId", key, value)
        logger.info("Send string message to Topic: " + s"topic_$sensorId" +", "+ record)
        producer.send(record)
        Thread.sleep(10000)
      }
    }
  }

  def writeRawData(path: String): Unit = {
    val d = getDF(path)
    d.show()
    var index = 2;
    val sensorList = Seq.range(54, 63) ++ Seq(24,52,64,67,70,73,76,79,82,85,88,91,93)
    println(sensorList)
    d.collect.foreach{ row =>
      val key = "%s".format(new DateTime())
      val value = "%s".format(convertRow2Json(row))
      if(sensorList.contains(index)) {
        val record = new ProducerRecord[String, String](s"topic_$index", key, value)
        logger.info("Send string message to Topic: " + s"topic_$index" +", "+ record)
        producer.send(record)
      }
      index += 1;
      if(index % 92 == 2) {
        Thread.sleep(10000)
        index = 2;
      }
    }

    producer.close
  }

}

object RawDataKafka {
  private[this] val bootStrapServers: String = "mymac:9092"
  private[this] val zooKeeperConnect: String = "localhost:2181"

  def apply(o: KafkaInfo): RawDataKafka = new RawDataKafka(o)

  private def kafkaIngestion = {
    val kafka = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("ingestion-group")
      .setTopic("ingestion")
      .build

    val path = "hdfs://localhost:9000/user/leeyh_etri_re_kr/dataset/input/rawEmulate.csv"
    RawDataKafka(kafka).writeData(path)
  }

  def main(args: Array[String]) = {
    kafkaIngestion
  }

}
