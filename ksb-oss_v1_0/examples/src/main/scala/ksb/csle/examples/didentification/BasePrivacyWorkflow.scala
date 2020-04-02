package ksb.csle.examples.didentification

import org.apache.spark.sql.DataFrame
import org.apache.logging.log4j.scala.Logging
import ksb.csle.tools.client._
import ksb.csle.common.utils.ProtoUtils
import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo._
import ksb.csle.common.base.workflow.BaseWorkflow

abstract class BasePrivacyWorkflow extends Logging {

  var operatorId: Int = 0
  def incrOperatorId: Int = {
    operatorId += 1
    (operatorId)
  }

  def submit(appId: String, workflow: WorkflowInfo) = {

    SimpleClient.submit(workflow)
  }

  def getFileReader(fileName: String): BatchReaderInfo = {
    val infileInfo = getDataFileInfo()
      .addFilePath(getPath(fileName))
      .build()

    BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
      .build
  }

  def getPath(path: String): String = {
    s"file:///${System.getProperty("user.dir")}/$path".replaceAll("\\\\", "/")
  }

  def getDataFileInfo(): FileInfo.Builder = {
    FileInfo.newBuilder
      .setFileType(FileInfo.FileType.CSV)
      .setHeader(true)
      .setDelimiter(";")
  }

  def makeColumn(
      colId: Int,
      colType: FieldInfo.FieldType,
      colName: String,
      attrType: AttrType): FieldInfo = {
    FieldInfo.newBuilder()
      .setKey(colId.toString())
      .setType(colType)
      .setValue(colName)
      .setAttrType(attrType)
      .build
  }

  def makeColumn(
      colId: Int,
      colType: FieldInfo.FieldType,
      colName: String,
      attrType: AttrType,
      path: String): FieldInfo = {
    FieldInfo.newBuilder()
      .setKey(colId.toString())
      .setType(colType)
      .setValue(colName)
      .setAttrType(attrType)
      .setAutoConfigured(false)
      .setFilePath(getPath(path))
      .build
  }

  def getFileWriter(fileName: String): BatchWriterInfo = {
    val outfileInfo = getDataFileInfo()
      .addFilePath(getPath(fileName))
      .build()

    BatchWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
      .build
  }

  def getBasePrivacyInfo(
      loss: LossMeasureMethod,
      risk: RiskMeasureMethod,
      check: CheckAnonymityMethod): PrivacyCheckInfo = {
    PrivacyCheckInfo.newBuilder()
      .setMeasureLoss(loss)
      .setMeasureRisk(risk)
      .setCheckAnonymity(check)
      .build()
  }

  def getDidentOperator(): StreamOperatorInfo.Builder = {
    StreamOperatorInfo.newBuilder()
      .setPrevId(operatorId-1)
      .setId(operatorId)
  }

  def getSimpleSparkRunner(appId: String): StreamRunnerInfo = {
    StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
          SparkRunnerInfo.newBuilder()
            .setSparkArgs(
              SparkArgs.newBuilder()
              .setAppName(appId)
              .setMaster("local[*]").build()
            ).build
        ).build
  }

  def getStreamController =
    StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)
      .build

  def getBatchToBatchStreamEngine(
      reader: BatchReaderInfo,
      operator: StreamOperatorInfo,
      runner: StreamRunnerInfo,
      controller: StreamControllerInfo,
      writer: BatchWriterInfo): BatchToBatchStreamEngineInfo = {
    BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator)
      .setRunner(runner)
      .setWriter(writer)
      .build
  }

  def getWorkflowInfo(engine: BatchToBatchStreamEngineInfo): WorkflowInfo = {
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
             .setBatchToBatchStreamEngine(engine))
      .build
  }
}
