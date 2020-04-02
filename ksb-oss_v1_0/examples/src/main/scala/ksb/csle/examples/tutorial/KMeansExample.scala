package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.tools.client._

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto._
import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.tools.client._
import ksb.csle.common.utils.config.ConfigUtils

/**
 * Object to create KMeans workflow scenario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}.
 * This is used for test of KMeans workflow scenario.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object KMeansExample extends Logging {
  val appId: String = "Data-KMeans"

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

    val controller = StreamControllerInfo.newBuilder()
      .setClsName(
          "ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(
          SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val infileInfo = FileInfo.newBuilder()
      .addFilePath(
        "dataset/input/input_kmeans.csv"
        .replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .addField(
        FieldInfo.newBuilder()
          .setKey("DATA1")
          .setType(FieldInfo.FieldType.DOUBLE)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("DATA2")
          .setType(FieldInfo.FieldType.DOUBLE)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("DATA3")
          .setType(FieldInfo.FieldType.INTEGER)
          .build())
          .addField(
        FieldInfo.newBuilder()
          .setKey("DATA4")
          .setType(FieldInfo.FieldType.DOUBLE)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("DATA5")
          .setType(FieldInfo.FieldType.DOUBLE)
          .build())
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)

    val kMeansInfo = KMeansInfo.newBuilder()
      .setKValue(2)
      .setMaxIterations(100)
      .setMaxRuns(1)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.cleaning.KMeansOperator")
      .setKMeans(kMeansInfo)
      .build

    val outfileInfo = FileInfo.newBuilder()
      .addFilePath("output/result_kmeans.csv"
        .replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .build
      val writer = BatchWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
          SparkRunnerInfo.getDefaultInstance)

    val dataEngineInfo = BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .addOperator(operator1)
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
            .setEngineNickName("DataEngine")
            .setBatchToBatchStreamEngine(dataEngineInfo))
      .build
  }
}
