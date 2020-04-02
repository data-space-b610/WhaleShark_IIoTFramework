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

object HttpToPostgresqlExample extends Logging {
  val appId: String = "HttpToPostgresqlExample"

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
    val pgsqlJdbcUrl = "jdbc:postgresql://localhost/foodb"
    val pgsqlDriver = "org.postgresql.Driver"
    val pgsqlTableName = "data_from_http"
    val pgsqlUserName = "foo"
    val pgsqlPassword = "foo"
    val pgsqlWriteMode = WriteMode.APPEND

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
      .setClsName(tableWriterClassName)
      .setTableWriter(
          TableInfo.newBuilder()
            .setJdbcUrl(pgsqlJdbcUrl)
            .setJdbcDriver(pgsqlDriver)
            .setTableName(pgsqlTableName)
            .setUserName(pgsqlUserName)
            .setPassword(pgsqlPassword)
            .setWriteMode(pgsqlWriteMode))

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
