package ksb.csle.examples.didentification

import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo._

object DidentificationExample extends BasePrivacyWorkflow {
  val appId: String = "Didentification Scenario"

  def main(args: Array[String]) {
    submit(appId, workflow)
  }

  private def workflow = {
    val reader = getFileReader("input/adult.csv")

    val loss = LossMeasureMethod.AECS
    val risk = RiskMeasureMethod.UNIQUENESS
    val anonymityCheck = CheckAnonymityMethod.KANONYMITYCHECK
    val privacyInfo = getBasePrivacyInfo(loss, risk, anonymityCheck)

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
    val operator1 = getDidentOperator()
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

    val operator2 = getDidentOperator()
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

    val operator3 = getDidentOperator()
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
    val operator4 = getDidentOperator()
      .setClsName("ksb.csle.didentification.privacy.EncryptionOperator")
      .setEncryption(encryptionInfo)
      .build

    val controller = getStreamController
    val runner = getSimpleSparkRunner(appId)
    val writer = getFileWriter(s"output/Aggregation.csv")

    val dataEngineInfo = BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .setRunner(runner)
      .setWriter(writer)
      .build

    getWorkflowInfo(dataEngineInfo)
  }
}
