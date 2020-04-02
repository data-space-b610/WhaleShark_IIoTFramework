package ksb.csle.examples.schedule

import java.util.Date
import java.text.SimpleDateFormat

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FileInfo._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

import ksb.csle.examples.ingestion._

object StreamEveryMinute extends Logging {
  val appId: String = "StreamEveryMinute"

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
    val format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    val runType = RunType.newBuilder()
      .setId(1)
      .setStartTime(format.format(new Date()))
      .setPeriodic(Periodic.EVERY_MINUTE)
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
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
        "servers.spark.master")
    val httpServerAddress = "0.0.0.0"
    val httpServerPort = 53001

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

    val writer = StreamWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(stdoutWriterClassName)
      .setStdoutWriter(StdoutWriterInfo.getDefaultInstance())

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName(runnerClassName)
      .setSparkRunner(
          SparkRunnerInfo.newBuilder()
            .setSparkArgs(
                SparkArgs.newBuilder()
                  .setAppName(appId)
                  .setMaster(masterMode)))

    StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setRunner(runner)
      .build()
  }
}
