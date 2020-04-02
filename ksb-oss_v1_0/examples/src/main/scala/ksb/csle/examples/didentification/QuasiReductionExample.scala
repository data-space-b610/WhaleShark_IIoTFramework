package ksb.csle.examples.didentification

import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo._

object QuasiReductionExample extends BasePrivacyWorkflow {
  val appId: String = "Quasi-Reduction"
  
  def main(args: Array[String]) {
    submit(appId, workflow)
  }
  
  private def workflow = {
    val reader = getFileReader("input/adult.csv")
      
    val loss = LossMeasureMethod.AECS
    val risk = RiskMeasureMethod.UNIQUENESS
    val anonymityCheck = CheckAnonymityMethod.KANONYMITYCHECK
    val privacyInfo = getBasePrivacyInfo(loss, risk, anonymityCheck)
      
    val reductionInfo = QuasiReductionInfo.newBuilder()
      .setCheck(privacyInfo)
      .addFieldInfo(makeColumn(
          0, FieldType.STRING, "sex", AttrType.IDENTIFIER, "input/adult_hierarchy_sex.csv"))
      .addFieldInfo(makeColumn(
          1, FieldType.INTEGER, "age", AttrType.QUASIIDENTIFIER, "input/adult_hierarchy_age.csv"))
      .addFieldInfo(makeColumn(
          2, FieldType.STRING, "race", AttrType.QUASIIDENTIFIER, "input/adult_hierarchy_race.csv"))
      .addFieldInfo(makeColumn(
          3, FieldType.STRING, "marital-status", AttrType.QUASIIDENTIFIER))
      .addFieldInfo(makeColumn(
          8, FieldType.STRING, "salary-class", AttrType.SENSITIVE))
      .setMethod(ReductionMethod.REPLACE)
    
    val safeHarborList = List("age")
    safeHarborList.map(col => reductionInfo.addSafeHarborList(col))
    
    val operator = getDidentOperator()
      .setClsName("ksb.csle.didentification.privacy.QuasiReductionOperator")
      .setQuasiReduction(reductionInfo)
      .build

    val controller = getStreamController
    val runner = getSimpleSparkRunner(appId)
    val writer = getFileWriter(s"output/QuasiReduction.csv")
      
    val dataEngineInfo = getBatchToBatchStreamEngine(
        reader, operator, runner, controller, writer)
      
    getWorkflowInfo(dataEngineInfo)
  }
}
