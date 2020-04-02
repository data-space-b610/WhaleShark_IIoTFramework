package ksb.csle.examples.energy

import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo._

import ksb.csle.tools.client.SimpleCsleClient
import ksb.csle.common.utils.config.ConfigUtils

abstract class EnergyWorkflowLibrary extends Logging {
  val appId: String = "EnergyWorkflow"

  val userHome = ConfigUtils.getConfig().envOrElseConfig("csle.user.home")
  val ksbHome = sys.env("KSB_HOME")
  val masterMode = ConfigUtils.getConfig().envOrElseConfig("servers.spark.master")
  val deployMode = ConfigUtils.getConfig().envOrElseConfig("servers.spark.deploy")
  val sparkCluster =
      SparkArgs.newBuilder()
      .setNumExecutors("3")
      .setDriverMemory("4g")
      .setExecuterMemory("4g")
      .setExecutorCores("2")
      .build

  val inputDir = System.getProperty("user.dir") + "/../examples"

  val phoenixJdbcUrl = "jdbc:phoenix:127.0.0.1:2181/hbase"
  val phoenixZkUrl = "127.0.0.1:2181"
  val phoenixWriteMode = WriteMode.APPEND
//  val phoenixWriteMode = WriteMode.OVERWRITE

  var operatorId: Int = 0
  def incrOperatorId: Int = {
    operatorId += 1
    (operatorId)
  }

  def submit(appId: String, workflow: WorkflowInfo) = {

    SimpleClient.submit(workflow)
  }

  def getHDFSPath(fetchTime: String): String = {
    val path = s"${fetchTime.replace(" ","").replace(":","").replace("-","")}"
    s"hdfs://csle1:9000/energy/data/$path.csv"
  }

  def getFileInfo(fileName: String): FileInfo = {
    FileInfo.newBuilder
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .setHeader(true)
      .addFilePath(fileName)
      .build()
  }

  def getFileReader(fileName: String): BatchReaderInfo = {
    BatchReaderInfo.newBuilder()
      .setPrevId(operatorId)
      .setId(incrOperatorId)
      .setFileReader(getFileInfo(fileName))
      .setClsName("ksb.csle.component.reader.FileReader")
      .build
  }

  def getFileWriter(fileName: String): BatchWriterInfo = {
    BatchWriterInfo.newBuilder()
      .setPrevId(operatorId)
      .setId(incrOperatorId)
      .setFileWriter(getFileInfo(fileName))
      .setClsName("ksb.csle.component.writer.FileWriter")
      .build
  }

  def getDidentOperator(): StreamOperatorInfo.Builder = {
    StreamOperatorInfo.newBuilder()
      .setPrevId(operatorId)
      .setId(incrOperatorId)
  }

  def makeColumn(
      colId: Int,
      colType: FieldInfo.FieldType,
      colName: String): FieldInfo = {
    FieldInfo.newBuilder()
      .setKey(colId.toString())
      .setType(colType)
      .setValue(colName)
      .build
  }

  def getPhoenixInfo(
      phoenixJdbcUrl: String,
      phoenixZkUrl: String,
      phoenixTableName: String) = {
    PhoenixInfo.newBuilder()
      .setJdbcUrl(phoenixJdbcUrl)
      .setZkUrl(phoenixZkUrl)
      .setTableName(phoenixTableName)
  }

  def getKafkaReader(
      bootStrapServers: String,
      zooKeeperConnect: String,
      group: String,
      topic: String): StreamReaderInfo = {
    val kafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId(group)
      .setTopic(topic)
      .build

    StreamReaderInfo.newBuilder()
      .setPrevId(operatorId)
      .setId(incrOperatorId)
      .setKafkaReader(kafkaInfo)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .build
  }

  def getKafkaWriter(
      bootStrapServers: String,
      zooKeeperConnect: String,
      group: String,
      topic: String): StreamWriterInfo = {
    val kafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId(group)
      .setTopic(topic)
      .build

    StreamWriterInfo.newBuilder()
      .setPrevId(operatorId)
      .setId(incrOperatorId)
      .setKafkaWriter(kafkaInfo)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .build
  }

  def makeKafkaPipeWriter(
      mode: String,
      bootStrapServers: String,
      zooKeeperConnect: String,
      topic: String,
      failOnDataLoss: Boolean,
      triggerTime: String,
      checkpoint: String): KafkaPipeWriterInfo = {
    KafkaPipeWriterInfo.newBuilder()
      .setMode(mode)
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic(topic)
      .setTrigger(triggerTime)
      .setFailOnDataLoss(failOnDataLoss)
      .setCheckpointLocation(checkpoint)
      .build
  }

}
