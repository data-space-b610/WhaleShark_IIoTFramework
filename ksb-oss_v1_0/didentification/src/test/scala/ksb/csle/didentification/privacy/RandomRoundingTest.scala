package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Random Rounding model
 */
class RandomRoundingTest extends PrivacyDataTest {

  "A RandomRounding" should "round string tuples with ROUND_UP" in
    confRounding(8, 10, RoundingMethod.ROUND_UP)

  it should "round string tuples with ROUND_DOWN" in
    confRounding(8, 5, RoundingMethod.ROUND_DOWN)

  it should "round string tuples with ROUND" in
    confRounding(8, 20, RoundingMethod.ROUND)

  it should "round numeric tuples with ROUND" in
    confRounding(1, 10, RoundingMethod.ROUND)

  private def confRounding(colId: Int, nStep: Int, method: RoundingMethod) = {
    val roundingInfo = RandomRoundingInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))       .addSelectedColumnId(colId)
      .setRoundStep(nStep)
      .setMethod(method)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.RandomRounding")
      .setRandomRounding(roundingInfo)
      .build

    val result = RandomRoundingOperator(operator).operate(data)
  }

}
