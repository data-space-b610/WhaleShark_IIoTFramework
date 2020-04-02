package ksb.csle.examples.ingestion.streamtobatch

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

import ksb.csle.examples.ingestion._

object HttpToPhoenixExample extends Logging {
  val appId: String = "HttpToPhoenixExample"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    Try (client.submit(workflowJson)) match {
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
            .setStreamToBatchEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val httpServerAddress = "0.0.0.0"
    val httpServerPort = 53001
    val phoenixJdbcUrl = "jdbc:phoenix:localhost:2181/hbase"
    val phoenixZkUrl = "localhost:2181"
    val phoenixTableName = "data_from_http"
    val phoenixWriteMode = WriteMode.APPEND

    val controller = StreamControllerInfo.newBuilder()
      .setClsName(streamToStreamControllerClassName)
      .setSparkSessionOrStreamController(
          SimpleBatchOrStreamControllerInfo.newBuilder()
            .setOperationPeriod(3))

    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(httpServerReaderClassName)
      .setHttpServerReader(
          HttpServerInfo.newBuilder()
            .setIp(httpServerAddress)
            .setPort(httpServerPort))

    val writer = BatchWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(phoenixWriterClassName)
      .setPhoenixWriter(
          PhoenixInfo.newBuilder()
            .setJdbcUrl(phoenixJdbcUrl)
            .setZkUrl(phoenixZkUrl)
            .setTableName(phoenixTableName)
            .setWriteMode(phoenixWriteMode))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName(runnerClassName)
      .setSparkRunner(
          SparkRunnerInfo.getDefaultInstance)

    StreamToBatchEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setRunner(runner)
      .build()
  }
}
