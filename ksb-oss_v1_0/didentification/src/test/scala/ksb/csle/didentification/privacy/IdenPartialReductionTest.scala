package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Iden-Partial Reduction model
 */
class IdenPartialReductionTest extends PrivacyDataTest {

  "An IdenPartialReduction" should
  "generalizes identifier columns (col: 1, max step: 5, cur step: 2" in
    confIdenPartialReduction(List("1"),
        List((1, 5, 2)),
        ReductionMethod.DELETE,
        ColumnHandlePolicy.ONEBYONE)

  "An IdenPartialReduction" should
  "generalizes identifier columns (col: 1, max step: 5, cur step: " in
    confIdenPartialReduction(List("1"),
        List((1, 5, 3)),
        ReductionMethod.DELETE,
        ColumnHandlePolicy.ONEBYONE)

  "An IdenPartialReduction" should
  "generalizes identifier columns (col: 1, 3, max step: 5, 10, cur step: 4, 2" in
    confIdenPartialReduction(List("1", "3"),
        List((1, 5, 4), (3, 5, 2)),
        ReductionMethod.DELETE,
        ColumnHandlePolicy.ONEBYONE)

  private def confIdenPartialReduction(
      colnumIDs: List[String],
      genColInfos: List[(Int, Int, Int)],
      method: ReductionMethod,
      policy: ColumnHandlePolicy) = {
    val reductionInfo = IdenPartialReductionInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))      .setMethod(method)
      .setColumnHandlePolicy(policy)

    colnumIDs.map(col => {
      reductionInfo.addFieldInfo(
        FieldInfo.newBuilder()
          .setKey(col)
          .setType(FieldInfo.FieldType.STRING)
          .setAttrType(FieldInfo.AttrType.IDENTIFIER)
          .build() 
      )
    })

    genColInfos.map(col => {
      reductionInfo.addGeneralizedColumnInfo(
        GeneralizeColumnInfo.newBuilder()
          .setSelectedColumnId(col._1)
          .setNumLevels(col._2)
          .setCurLevel(col._3)
      )
    })

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.IdenPartialReduction")
      .setIdenPartial(reductionInfo)
      .build

    val resultDF = PartialIdenReductionOperator(operator).operate(data)
  }

}
