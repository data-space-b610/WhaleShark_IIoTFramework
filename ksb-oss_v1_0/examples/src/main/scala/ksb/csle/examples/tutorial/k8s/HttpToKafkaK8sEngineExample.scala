package ksb.csle.examples.tutorial.k8s

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.OndemandOperatorProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.AutoSparkMlProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.examples.tutorial._

import ksb.csle.tools.client._

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object HttpToKafkaK8sEngineExample extends Logging {
  val appId: String = "RealtimeIngestToPredictInSingleEngine"

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
     WorkflowInfo.newBuilder()
     .setBatch(false)
     .setMsgVersion("v1.0")
     .setKsbVersion("v1.0")
     .addEngines(
       EngineInfo.newBuilder()
       .setId(1)
       .setPrevId(0)
       .setEngineNickName("k8s")
       .setStreamToStreamEngine(getEngine))
      .addRuntypes(
        RunType.newBuilder()
       .setId(1)
       .setPeriodic(Periodic.ONCE))
     .build()
  }

  private[this] val sparkCluster =
      SparkArgs.newBuilder()
      .setExtraArgs(userHome + "/hadoop/etc/hadoop/")
      .setNumExecutors("2")
      .setDriverMemory("2g")
      .setExecuterMemory("2g")
      .setExecutorCores("2")
      .build

  val getEngine: StreamToStreamEngineInfo = {
    val inHttpServerInfo = HttpServerInfo.newBuilder()
      .setIp("0.0.0.0")
      .setPort(53002)
      .build()
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.HttpServerReader")
      .setHttpServerReader(inHttpServerInfo)
      .build()
   val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers("csle1:9092")
      .setZooKeeperConnect("csle1:2181")
      .setGroupId("group1")
      .setTopic("test1")
      .build
    val writer = StreamWriterInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(2)
            .setWindowSize(10)
            .setSlidingSize(1))

    val ingestToPredicInfo = StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setRunner(runner)
      .build

    ingestToPredicInfo
  }
}
