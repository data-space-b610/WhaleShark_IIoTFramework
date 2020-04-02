package ksb.csle.examples.tutorial

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

object TfStreamPredictionMnist extends Logging {
  val appId = "TfStreamPredictionMnist"

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
    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()
    WorkflowInfo.newBuilder()
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("StreamIngestionEngine")
            .setStreamToStreamEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val httpServerPort = 53010
    val kafkaServer = "csle1:9092"
    val kafkaZookeeper = "csle1:2181"

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(
        SimpleBatchOrStreamControllerInfo.newBuilder()
          .setOperationPeriod(1))

    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(
          KafkaInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setGroupId("mnist_input")
            .setTopic("mnist_input"))

    // grpc:// or file:// or hdfs://
    val modelBasePath = "file:///home/csle/ksb-csle/examples/models/mnist/model"
    val operator = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.analysis.TensorflowPredictOperator")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri(modelBasePath)
          .setModelName("mnist")
          .setSignatureName("predict_images"))
      .build()

    val writer = StreamWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(
        KafkaInfo.newBuilder()
          .setBootStrapServers(kafkaServer)
          .setZooKeeperConnect(kafkaZookeeper)
          .setGroupId("mnist_output")
          .setTopic("mnist_output"))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)

    StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator)
      .setWriter(writer)
      .setRunner(runner)
      .build()
  }
}
