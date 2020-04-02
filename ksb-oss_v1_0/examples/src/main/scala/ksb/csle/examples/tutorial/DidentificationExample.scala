package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo._
import ksb.csle.common.utils.ProtoUtils

import ksb.csle.tools.client._

object DidentificationExample extends Logging {
  val appId: String = "Didentification Scenario"
  val sparkCluster =
      SparkArgs.newBuilder()
      .setNumExecutors("3")
      .setDriverMemory("4g")
      .setExecuterMemory("4g")
      .setExecutorCores("2")
      .build

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
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
         EngineInfo.newBuilder()
           .setId(1)
           .setPrevId(0)
           .setEngineNickName("Deidentification Engine")
           .setBatchToBatchStreamEngine(anonymize))
     .build
  }

  private def anonymize = {
    val inPath = "dataset/input/adult.csv"
    val infileInfo = FileInfo.newBuilder
      .setFileType(FileInfo.FileType.CSV)
      .setHeader(true)
      .setDelimiter(";")
      .addFilePath(inPath)
      .build()

    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
      .build

    val privacyInfo = PrivacyCheckInfo.newBuilder()
      .setMeasureLoss(LossMeasureMethod.AECS)
      .setMeasureRisk(RiskMeasureMethod.UNIQUENESS)
      .setCheckAnonymity(CheckAnonymityMethod.KANONYMITYCHECK)
      .build()

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

    val aggrInfo = AggregationInfo.newBuilder()
      .setCheck(privacyInfo)
      .addFieldInfo(makeColumn(
          0, FieldType.STRING, "sex", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          1, FieldType.INTEGER, "age", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          2, FieldType.STRING, "race", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          3, FieldType.STRING, "marital-status", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          8, FieldType.STRING, "salary-class", AttrType.SENSITIVE))
      .addSelectedColumnId(1)
      .setMethod(AggregationMethod.AVG)
      .build()
    val operator1 = StreamOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.didentification.privacy.AggregationOperator")
      .setAggregation(aggrInfo)
      .build

    val heuristicInfo = HeuristicInfo.newBuilder()
      .setCheck(privacyInfo)
      .addFieldInfo(makeColumn(
          0, FieldType.STRING, "sex", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          1, FieldType.INTEGER, "age", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          2, FieldType.STRING, "race", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          3, FieldType.STRING, "marital-status", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          8, FieldType.STRING, "salary-class", AttrType.SENSITIVE))
      .addSelectedColumnId(2)
      .setMethod(GenHeuristicTableMethod.HEUR_RANDOM)
      .setRandInfo(RandomInfo.newBuilder()
          .setRandMethod(RandomMethod.MIXED)
          .setLength(5)
          .build)
      .build()

    val operator2 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.didentification.privacy.HeuristicOperator")
      .setHeuristic(heuristicInfo)
      .build

    val reductionInfo = IdenPartialReductionInfo.newBuilder()
      .setCheck(privacyInfo)
      .addFieldInfo(makeColumn(
          0, FieldType.STRING, "sex", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          1, FieldType.INTEGER, "age", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          2, FieldType.STRING, "race", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          3, FieldType.STRING, "marital-status", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          8, FieldType.STRING, "salary-class", AttrType.SENSITIVE))
      .setMethod(ReductionMethod.DELETE)
      .setColumnHandlePolicy(ColumnHandlePolicy.ONEBYONE)
      .addGeneralizedColumnInfo(
        GeneralizeColumnInfo.newBuilder()
          .setSelectedColumnId(3)
          .setNumLevels(4)
          .setCurLevel(2)
          .build)
      .build

    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.didentification.privacy.PartialIdenReductionOperator")
      .setIdenPartial(reductionInfo)
      .build

    val encryptionInfo = EncryptionInfo.newBuilder()
      .setCheck(privacyInfo)
      .addFieldInfo(makeColumn(
          0, FieldType.STRING, "sex", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          1, FieldType.INTEGER, "age", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          2, FieldType.STRING, "race", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          3, FieldType.STRING, "marital-status", AttrType.IDENTIFIER))
      .addFieldInfo(makeColumn(
          8, FieldType.STRING, "salary-class", AttrType.SENSITIVE))
      .addSelectedColumnId(7)
      .setKey(EncryptionKey.SHA1)
      .setMethod(EncryptionMethod.AES)
      .build()
    val operator4 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.didentification.privacy.EncryptionOperator")
      .setEncryption(encryptionInfo)
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)
      .build

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
          SparkRunnerInfo.newBuilder()
            .setSparkArgs(
              SparkArgs.newBuilder()
              .setAppName(appId)
              .setMaster("local[*]").build()
            ).build
        ).build

    val outPath = "output/deidentification.csv"
    val outfileInfo = FileInfo.newBuilder
      .setFileType(FileInfo.FileType.CSV)
      .setHeader(true)
      .setDelimiter(";")
      .addFilePath(outPath)
      .build()
    val writer = BatchWriterInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
      .build

    BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .setRunner(runner)
      .setWriter(writer)
      .build
  }
}
