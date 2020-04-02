package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for IdenReduction model
 */
class IdenReductionTest extends PrivacyDataTest {

  /*"An IdenReduction" should "drop columns of identifiers" in {
    val fieldInfo = FieldInfo.newBuilder()
      .setKey("0")
      .setType(FieldInfo.FieldType.STRING)
      .setAttrType(FieldInfo.AttrType.IDENTIFIER)

    val reductionInfo = IdenReductionInfo.newBuilder()
      .setPrivacyInfo(getBasePrivacyInfo(BasicDidentType.IDENREDUCTION))
      .setMethod(ReductionMethod.DELETE)
      .addFieldInfo(fieldInfo)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(reductionInfo.getPrivacyInfo.getOpId)
      .setPrevId(reductionInfo.getPrivacyInfo.getOpId - 1)
      .setOpType(OpType.DEIDENTIFICATION)
      .setClsName("ksb.csle.didentification.privacy.IdenReduction")
      .setIdenReduction(reductionInfo)
      .build

    val resultDF = IdenReductionOperator(operator).operate(data)
    resultDF.show
  }*/

  "An IdenReduction" should "drop specific columns " in {
    val reductionInfo = IdenReductionInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))      .setMethod(ReductionMethod.DELETE)
//      .addSelectedColumnId(0)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.IdenReduction")
      .setIdenReduction(reductionInfo)
      .build

    val resultDF = IdenReductionOperator(operator).operate(data)
  }

}
