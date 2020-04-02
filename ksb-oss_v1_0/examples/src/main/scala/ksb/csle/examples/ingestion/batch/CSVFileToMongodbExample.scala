package ksb.csle.examples.ingestion.batch

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

object CSVFileToMongodbExample extends Logging {
  val appId: String = "CSVFileToMongodbExample"

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
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("BatchIngestionEngine")
            .setBatchDummyEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val inputDir = System.getProperty("user.dir") + "/../examples"
    val csvFilePath = s"file:///$inputDir/input/NOAA_NORMAL_HLY_sample.csv"
    val mongoServerAddr = "localhost:27017"
    val mongoDBName = "examples"
    val mongoCollectionName = "NOAA_NORMAL_HLY_sample_from_csv"

    val controller = BatchControllerInfo.newBuilder()
      .setClsName(batchDummyControllerClassName)
      .setBatchDummyController(
          SimpleBatchControllerInfo.newBuilder())

    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(fileReaderClassName)
      .setFileReader(
          FileInfo.newBuilder()
            .addFilePath(csvFilePath)
            .setFileType(FileType.CSV)
            .setHeader(true))

    val writer = BatchWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(mongodbWriterClassName)
      .setMongodbWriter(
          MongodbInfo.newBuilder()
            .setServerAddress(mongoServerAddr)
            .setDbName(mongoDBName)
            .setCollectionName(mongoCollectionName))

    val runner = BatchRunnerInfo.newBuilder()
      .setClsName(runnerClassName)
      .setSparkRunner(
          SparkRunnerInfo.getDefaultInstance)

    BatchDummyEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setRunner(runner)
      .build()
  }
}
