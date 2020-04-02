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

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object TimeSynchronizeExample extends Logging {
  val appId: String = "Data-TimeSynchronizeExample"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    client.submit(workflowJson)
    client.close()
  }

  private def workflow = {
    val infileInfo = FileInfo.newBuilder()
        .addFilePath(s"file:///${System.getProperty("user.dir")}/input/input_timetest.csv".replaceAll("\\\\", "/"))
        .setFileType(FileInfo.FileType.CSV)
        .setHeader(true)
        .setDelimiter(",")
        .addField(
          FieldInfo.newBuilder()
          .setKey("time")
          .setType(FieldInfo.FieldType.TIMESTAMP)
          .build())
        .addField(
          FieldInfo.newBuilder()
          .setKey("value")
          .setType(FieldInfo.FieldType.INTEGER)
          .build())
        .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
      .build

    val interpolation1 = InterpolationInfo.newBuilder()
      .setIndexColumnName("time")
      .setStepSize(3600)
      .setMethodOption(InterpolationMethodOption.PREV_COPY)
    val interpolation2 = InterpolationInfo.newBuilder()
      .setIndexColumnName("time2")
      .setStepSize(3600)
      .setMethodOption(InterpolationMethodOption.LINEAR)
    val joinDataInfo = TimeSyncDataFrameInfo.newBuilder()
      .setFilePath(s"file:///${System.getProperty("user.dir")}/input/input_timetest2.csv".replaceAll("\\\\", "/"))
//      .setHasHeader(true)
//      .setSeparator(",")
      .setInterpolationInfo(interpolation2)
      .setIsTimeStampColumn(true)
    val timeSyncInfo = TimeSynchronizationInfo.newBuilder()
      .setHasTimeDelay(true)
      .setInterpolationInfo(interpolation1)
      .setIsTimeStampColumn(true)
      .setJoinDataInfo(joinDataInfo)
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.transformation.TimeSynchronizeOperator")
      .setTimeSynchronization(timeSyncInfo)
      .build

    val outfileInfo = FileInfo.newBuilder()
        .addFilePath("result_timesync")
        .setFileType(FileInfo.FileType.CSV)
        .setDelimiter(",")
        .setHeader(true)
        .build
    val writer = BatchWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
      .build

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
