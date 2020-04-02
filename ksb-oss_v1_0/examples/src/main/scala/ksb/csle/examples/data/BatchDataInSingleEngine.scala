package ksb.csle.examples.data

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.utils.ProtoUtils

import ksb.csle.tools.client._

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object BatchDataInSingleEngine extends Logging {

  private val appId: String = "MyFirstWorkflow"
  private[this] val ksbHome = sys.env("KSB_HOME")

  def main(args: Array[String]) =  {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    Try (client.submit(
        workflowJson,
        "leeyh@etri.re.kr",
        this.getClass.getSimpleName.replace("$",""),
        this.getClass.getSimpleName.replace("$",""))) match {
      case Success(id) => logger.info("submit success:" + id)
      case Failure(e) => logger.error("submit error", e)
    }
    client.close()
  }

  private def workflow = {
    val infileInfo = FileInfo.newBuilder()
      .addFilePath(s"dataset/${this.getClass.getSimpleName.replace("$", "")}/input.csv".replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .addField(
        FieldInfo.newBuilder()
          .setKey("word")
          .setType(FieldInfo.FieldType.STRING)
          .build())
      .addField(
        FieldInfo.newBuilder()
          .setKey("count")
          .setType(FieldInfo.FieldType.INTEGER)
          .build())

      .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo).build

    val filterInfo = FilterInfo.newBuilder()
      .setColName("count")
      .setCondition(FilterInfo.Condition.LESS_THAN)
      .setValue(8)
      .build()
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.reduction.FilterOperator")
      .setFilter(filterInfo)
      .build

    val groupbyInfo = GroupbyInfo.newBuilder()
      .setKeyColName("word")
      .setValColName("count")
      .setGroupby(GroupbyInfo.GroupbyOp.SUM)
      .build()
    val operator2 = StreamOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.operator.integration.GroupByOperator")
      .setGroupby(groupbyInfo)
      .build

    val outfileInfo = FileInfo.newBuilder()
      .addFilePath(s"output/${appId}".replaceAll("\\\\", "/"))
      .setFileType(FileInfo.FileType.CSV)
      .build
    val writer = BatchWriterInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo).build

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(
        SimpleBatchOrStreamControllerInfo.newBuilder())

    val dataEngineInfo = BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .addOperator(operator1)
      .addOperator(operator2)
      .setRunner(runner)
      .build

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addEngines(
        EngineInfo.newBuilder()
        .setId(1)
        .setPrevId(0)
        .setEngineNickName("DataEngine")
        .setBatchToBatchStreamEngine(dataEngineInfo))
      .addRuntypes(
        RunType.newBuilder()
       .setId(1)
       .setPeriodic(Periodic.ONCE))
       .setVerbose(true)
      .build
  }
}
