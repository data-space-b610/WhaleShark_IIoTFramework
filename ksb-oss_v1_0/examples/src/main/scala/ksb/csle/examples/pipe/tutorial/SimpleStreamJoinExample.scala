package ksb.csle.examples.pipe.tutorial

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.examples.SimpleClient

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object SimpleStreamJoinExample extends App {
  val appId: String = "SimpleStreamJoinExample"

  SimpleClient.submit(workflow)

  private def workflow = {
    val kafkaReaderClassName = "ksb.csle.component.pipe.stream.reader.KafkaPipeReader"
    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "group1"
    val kafkaTopic = List("topic1", "topic2")

    val reader1 = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(kafkaReaderClassName)
      .setKafkaPipeReader(
          KafkaPipeReaderInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setTopic(kafkaTopic(0))
            .setFailOnDataLoss(false)
            .setSampleJsonPath(s"file:///${System.getProperty("user.dir")}/input/thermoStreamSample.json")
            .setAddTimestamp(true)
            .setTimestampName("timestamp")
            .setWatermark("1 minutes"))
    val reader2 = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(kafkaReaderClassName)
      .setKafkaPipeReader(
          KafkaPipeReaderInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setTopic(kafkaTopic(1))
            .setFailOnDataLoss(false)
            .setSampleJsonPath(s"file:///${System.getProperty("user.dir")}/input/hygroStreamSample.json")
            .setAddTimestamp(true)
            .setTimestampName("timestamp")
            .setWatermark("1 minutes"))
    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.pipe.stream.operator.StreamAllJoinOperator")
      .setAllJoin(
          AllJoinPipeInfo.newBuilder()
          .setKey("timestamp")
          .build())
    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.pipe.stream.operator.GroupByOperator")
      .setGroupby(
          GroupbyPipeInfo.newBuilder()
          .addKeyColName("thermoId")
          .addValColName("thermoType")
          .setGroupby(GroupbyPipeInfo.GroupbyOp.COUNT)
          .setWindow(
              Window.newBuilder()
              .setKey("timestamp")
              .setWindowLength("2 minutes")
              .setSlidingInterval("1 minutes"))
          .build())
      .build
    val operator3 = StreamPipeOperatorInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.pipe.stream.operator.FilterOperator")
      .setFilter(
          FilterPipeInfo.newBuilder()
          .setColName("thermoId")
          .setCondition(FilterPipeInfo.Condition.LIKE)
          .setPattern("thermoId_01")
          .build())
      .build
    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.pipe.stream.writer.ConsolePipeWriter")
      .setConsolePipeWriter(
          ConsolePipeWriterInfo.newBuilder()
          .setMode("append")
          .setTrigger("2 seconds"))
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val dataEngineInfo = StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader1)
      .addReader(reader2)
      .addWriter(writer1)
      .addOperator(operator1)
      .addOperator(operator2)
//      .addOperator(operator3)
      .setRunner(runner)
      .build

    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("StreamJoinEngine")
            .setStreamJoinEngine(dataEngineInfo))
      .build
  }
}
