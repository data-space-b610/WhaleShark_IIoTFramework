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
object SimpleStreamWithNoOperationExample extends App {
  val appId: String = "SimpleStreamWithNoOperationExample"

  SimpleClient.submit(workflow)

  private def workflow = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
        "servers.spark.master")
    val kafkaReaderClassName = "ksb.csle.component.pipe.stream.reader.KafkaPipeReader"
    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "group1"
    val kafkaTopic = "topic1"

    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(kafkaReaderClassName)
      .setKafkaPipeReader(
          KafkaPipeReaderInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setTopic(kafkaTopic)
            .setSampleJsonPath(s"file:///${System.getProperty("user.dir")}/input/thermoStreamSample.json")
            .setAddTimestamp(false)
            .setFailOnDataLoss(false))
    val writer = StreamPipeWriterInfo.newBuilder()
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

    val dataEngineInfo = StreamPipeEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
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
          .setEngineNickName("StreamPipeEngine")
          .setStreamPipeEngine(dataEngineInfo))
      .build
  }
}
