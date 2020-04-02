package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Heuristic model
 */
class HeuristicTest extends PrivacyDataTest {

  "An Heuristic" should "pseudo-anonymize tuples using Randwom with Alphabet" in
    confRandHeuristic(1, GenHeuristicTableMethod.HEUR_RANDOM, 2, List())

  it should "pseudo-anonymize tuples using Random with Mixed String" in
    confRandHeuristic(2, GenHeuristicTableMethod.HEUR_RANDOM, 5, List())

  it should "pseudo-anonymize tuples manullay" in
    confRandHeuristic(2, GenHeuristicTableMethod.HEUR_MANUAL, 2,
        List(("White", "KOREAN"), ("Black", "JAPAN")))

  private def confRandHeuristic(
      colId: Int,
      method: GenHeuristicTableMethod,
      length: Int,
      manualList: List[(String, String)]) = {
    val heuristicInfo = HeuristicInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))      .addSelectedColumnId(colId)
      .setMethod(method)

    method match {
      case GenHeuristicTableMethod.HEUR_RANDOM =>
        heuristicInfo.setRandInfo(RandomInfo.newBuilder()
          .setRandMethod(RandomMethod.MIXED)
          .setLength(length)
          .build)
      case GenHeuristicTableMethod.HEUR_MANUAL => {
        manualList.map(x => heuristicInfo.addManualInfo(
          ManualInfo.newBuilder()
          .setValue(x._1)
          .setReplaceValue(x._2)))
      }
    }

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.Heuristic")
      .setHeuristic(heuristicInfo)
      .build

    val result = HeuristicOperator(operator).operate(data)
  }

}
