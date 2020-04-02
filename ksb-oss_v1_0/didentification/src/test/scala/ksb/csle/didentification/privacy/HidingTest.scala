package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Hiding model
 */
class HidingTest extends PrivacyDataTest {

  "An Hiding" should "hide tuples of numeric column with average" in
    confHiding(1, AggregationMethod.MAX)

  it should "hide tuples of string column" in
    confHiding(2, AggregationMethod.AVG)

  it should "hide tuples of string column2" in
    confHiding(3, AggregationMethod.AVG)

  it should "hide tuples of string column containing number with average" in
    confHiding(8, AggregationMethod.AVG)

  it should "hide tuples of string column containing number with min" in
    confHiding(8, AggregationMethod.MIN)

  private def confHiding(colId: Int, method: AggregationMethod) = {
    val hidingInfo = HidingInfo.newBuilder()
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
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.Hiding")
      .setHiding(hidingInfo)
      .build

    val result = HidingOperator(operator).operate(data)
  }

}
