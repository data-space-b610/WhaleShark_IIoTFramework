package ksb.csle.examples.tutorial.e2e_traffic

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

object TrafficPreprocessing extends Logging {
  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])
    val client = new SimpleCsleClient("221.149.215.164", 19999)
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

    WorkflowInfo.newBuilder()
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .setVerbose(true)
      .addRuntypes(
          RunType.newBuilder()
            .setId(1)
            .setPeriodic(Periodic.ONCE))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("PreprocessingEngine")
            .setStreamToBatchEngine(preprocessingParam))
      .build()
  }

  private def preprocessingParam = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(10)
            .setWindowSize(170*9)
            .setSlidingSize(170*8))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(SparkRunnerInfo.newBuilder()
          .setSparkArgs(SparkArgs.newBuilder()
              .setDriverMemory("1g")
              .setExecuterMemory("1g")))

    //python kangnam_producer.py /home/csle/ksb-csle/examples/input/201601_kangnam.csv localhost:9092 traffic 0.1
    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "traffic"
    val kafkaTopic = "traffic"
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(
          KafkaInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setGroupId(kafkaGroupId)
            .setTopic(kafkaTopic))

    val operator1 = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(SelectColumnsInfo.newBuilder()
          .addAllSelectedColumnName(
              Seq("PRCS_DATE","LINK_ID","PRCS_SPD").asJava))
      .build()

    val operator2 = StreamOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.operator.reduction.GroupByFilterOperator")
      .setGroupbyFilter(GroupbyFilterInfo.newBuilder()
          .setKeyColName("PRCS_DATE")
          .setGroupby(GroupbyFilterInfo.GroupbyOp.COUNT)
          .setCondition(GroupbyFilterInfo.Condition.EQUAL)
          .setValue(170)
          )
      .build()

    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.operator.reduction.OrderByFilterOperator")
      .setOrderbyFilter(OrderbyFilterInfo.newBuilder()
          .setKeyColName("PRCS_DATE")
          .setMethod(OrderbyFilterInfo.Method.ASC)
          .setValue(170*8)
          )
      .build()

    val operator4 = StreamOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(MinMaxScalingInfo.newBuilder()
          .addSelectedColumnId(2) // PRCS_SPD
          .setMax("0.5")
          .setMin("-0.5")
          .setWithMinMaxRange(true)
          .setMaxRealValue("100")
          .setMinRealValue("0"))
      .build()

    val operator5 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.component.operator.transformation.PivotOperator")
      .setPivot(PivotInfo.newBuilder()
          .setSelectedColumnId(1) // LINK_ID
          .setGroupByColumn("0") // PRCS_DATE
          .setValueColumn("2") // PRCS_SPD
          .setMethod(PivotInfo.Method.AVG))
      .build()

    val columnIdPath = s"file:///home/csle/ksb-csle/examples/input/traffic_kangnam_cols2.txt"
    val operator6 = StreamOperatorInfo.newBuilder()
      .setPrevId(5)
      .setId(6)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectWithFileOperator")
      .setSelectColumnsWithFile(
          SelectColumnsWithFileInfo.newBuilder()
          .setColumnIdPath(columnIdPath))
      .build()

    val outfileInfo = FileInfo.newBuilder()
        .addFilePath(s"file:///home/csle/ksb-csle/examples/output/traffic_processing.csv")
        .setFileType(FileInfo.FileType.CSV)
        .setSaveMode(FileInfo.SaveMode.APPEND)
        .build
    val writer = BatchWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)

    StreamToBatchEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .setRunner(runner)
      .build()
  }

}
