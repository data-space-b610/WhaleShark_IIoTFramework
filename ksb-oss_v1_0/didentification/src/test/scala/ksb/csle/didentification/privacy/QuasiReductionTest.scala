package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Quasi-Reduction model
 */
class QuasiReductionTest extends PrivacyDataTest {

  "An QuasiReduction" should "drop columns of quasi-identifiers" in
    confQuasiReduction(ReductionMethod.DELETE, List())

  it should "replace columns of quasi-identifiers" in
    confQuasiReduction(ReductionMethod.REPLACE, List("sex", "age"))

  private def confQuasiReduction(
      method: ReductionMethod,
      colNames: List[String]) = {
    val reductionInfo = QuasiReductionInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))       .setMethod(method)

    colNames.map(col => reductionInfo.addSafeHarborList(col))

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.QuasiReduction")
      .setQuasiReduction(reductionInfo)
      .build

    val resultDF = QuasiReductionOperator(operator).operate(data)
  }

}
