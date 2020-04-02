package ksb.csle.examples.tutorial

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
import ksb.csle.common.utils.resolver.PathResolver

object HttpToMongodbExample extends Logging {
  val appId: String = "HttpToMongodbExample"
  private[this] val ksbHome = sys.env("KSB_HOME")

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
            .setStreamToBatchEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val httpServerAddress = "0.0.0.0"
    val httpServerPort = 53001
    val mongoServerAddr = "localhost:27017"
    val mongoDBName = "examples"
    val mongoCollectionName = "data_from_http"

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
      .setClsName(mongodbWriterClassName)
      .setMongodbWriter(
          MongodbInfo.newBuilder()
            .setServerAddress(mongoServerAddr)
            .setDbName(mongoDBName)
            .setCollectionName(mongoCollectionName))

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
