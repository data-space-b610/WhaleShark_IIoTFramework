package ksb.csle.examples.tutorial

import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}
import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.BatchControlProto._
import ksb.csle.common.proto.BatchOperatorProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.tools.client._

object TrafficStreamingFirstEngineExample2 extends Logging {
  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])
    val client = new SimpleCsleClient("localhost", 19999)
    Try (client.submit(
        workflowJson,
        "ksbuser@etri.re.kr",
        this.getClass.getSimpleName.replace("$",""),
        this.getClass.getSimpleName.replace("$",""))) match {
      case Success(id) => logger.info("submit success:" + id)
      case Failure(e) => logger.error("submit error", e)
    }
    client.close()
  }

  private def workflow = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")

    WorkflowInfo.newBuilder()
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .setVerbose(false)
      .addRuntypes(
          RunType.newBuilder()
            .setId(1)
            .setPeriodic(Periodic.ONCE))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("StreamProcessingEngine")
            .setStreamJoinEngine(engin1Param))
      .build()
  }

  private def engin1Param = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")

    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "traffic"
    val kafkaTopic = "traffic"

    //python kangnam_producer.py /home/csle/ksb-csle/examples/input/201601_kangnam_orgarnized_new.csv localhost:9092 traffic 0.01
    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.pipe.stream.reader.KafkaPipeReader")
      .setKafkaPipeReader(
          KafkaPipeReaderInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setTopic(kafkaTopic)
            .setFailOnDataLoss(false)
            .setSampleJsonPath(s"file:///home/csle/ksb-csle/examples/input/trafficStreamingSplitSample.json")
            .setAddTimestamp(false)
            .setTimestampName("PRCS_DATE")
            .setWatermark("2 minutes"))

     val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.pipe.stream.writer.ConsolePipeWriter")
      .setConsolePipeWriter(
          ConsolePipeWriterInfo.newBuilder()
          .setMode("append")
          .setTrigger("5 seconds"))

    val writer2 = StreamPipeWriterInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(
          KafkaPipeWriterInfo.newBuilder()
          .setMode("append")
          .setBootStrapServers(kafkaServer)
          .setZooKeeperConnect(kafkaZookeeper)
          .setCheckpointLocation(s"file:///tmp/kangnam/checkpoint/kafka1")
          .setTopic("traffic_output1")
//          .setKeyColumn("window")
          .setFailOnDataLoss(true)
          .setTrigger("5 seconds"))

    val csvFilePath = s"file:///tmp/kangnam/output/traffic1"
    val outFileInfo = FilePipeWriterInfo.newBuilder()
      .setCheckpointLocation(s"file:///tmp/kangnam/checkpoint/file1")
      .setMode("append")
      .setTrigger("5 seconds")
      .setFileInfo(FileInfo.newBuilder
        .setFileType(FileInfo.FileType.CSV)
        .setDelimiter(",")
        .setHeader(true)
        .addFilePath(csvFilePath))
    val writer3 = StreamPipeWriterInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.pipe.stream.writer.FilePipeWriter")
      .setFilePipeWriter(outFileInfo)
      .build

    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.pipe.stream.operator.GroupByOperator")
      .setGroupby(
          GroupbyPipeInfo.newBuilder()
          .setTimeColName("PRCS_DATE")
          .addKeyColName("LINK_ID")
          .addValColName("PRCS_SPD")
          .setGroupby(GroupbyPipeInfo.GroupbyOp.AVG)
          .setWindow(
              Window.newBuilder()
              .setKey("PRCS_DATE")
              .setWindowLength("1 minutes")
              .setSlidingInterval("30 seconds"))
          .build())
      .build

    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.pipe.stream.operator.SelectColumnsPipeOperator")
      .setSelectColumns(
          SelectColumnsPipeInfo.newBuilder()
            .addColName("LINK_ID")
            .addColName("window.start")
            .addColName("PRCS_SPD"))

    val operator3 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.pipe.stream.operator.RenameColumnsPipeOperator")
      .setRenameCol(
          RenameColumnsPipeInfo.newBuilder()
            .addSelectedColumn(
                SelectedColumnInfo.newBuilder()
                  .setSelectedColIndex(1)
                  .setNewColName("PRCS_DATE")
                  .setNewFieldType(FieldType.STRING))
      )

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addWriter(writer2)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .setRunner(runner)
      .build()
  }
}
