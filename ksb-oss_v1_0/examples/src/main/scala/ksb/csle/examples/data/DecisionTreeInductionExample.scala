package ksb.csle.examples.data

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
import ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo.ImpurityType

/**
 * Object to create DecisionTreeInduction workflow scenario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}.
 * This is used for test of DecisionTreeInduction workflow scenario.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object DecisionTreeInductionExample extends Logging {
  val appId: String = "Data-DecisionTreeInductionExample"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    client.submit(workflowJson)
    client.close()
  }

  private def workflow = {
    val decisionTreeInductionInfo =
      DecisionTreeInductionInfo.newBuilder()
        .setLabelName("Play")
        .setMaxDepth(20)
        .setMinInfoGain(0.1d)
        .setMinInstancesPerNode(2)
        .setMaxBins(32)
        .setCacheNodeIds(true)
        .setCheckpointInterval(-1)
        .setImpurityType(ImpurityType.GINI)
        .build
    val infileInfo = FileInfo.newBuilder()
      .addFilePath(
        s"file:///${System.getProperty(
            "user.dir")}/input/input_decisionTreeInduction.csv"
        .replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .addField(
          FieldInfo.newBuilder()
            .setKey("Play")
            .setType(FieldInfo.FieldType.STRING)
            .build())
      .addField(
          FieldInfo.newBuilder()
            .setKey("Outlook")
            .setType(FieldInfo.FieldType.STRING)
            .build())
      .addField(
          FieldInfo.newBuilder()
            .setKey("Temperature")
            .setType(FieldInfo.FieldType.INTEGER)
            .build())
      .addField(
          FieldInfo.newBuilder()
            .setKey("Humidity")
            .setType(FieldInfo.FieldType.INTEGER)
            .build())
      .addField(
          FieldInfo.newBuilder()
            .setKey("Wind")
            .setType(FieldInfo.FieldType.BOOLEAN)
            .build())
      .build
    val outfileInfo = FileInfo.newBuilder()
      .addFilePath(
        s"file:///${System.getProperty("user.dir")}/output/result_decisionTreeInduction.csv"
        .replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.reduction.DecisionTreeInductOperator")
      .setDecisionTreeInduction(decisionTreeInductionInfo)
      .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
    val writer = BatchWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
          SparkRunnerInfo.getDefaultInstance)
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)
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