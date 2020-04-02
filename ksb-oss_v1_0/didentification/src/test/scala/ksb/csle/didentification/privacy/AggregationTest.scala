package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Aggregation model in Aggregation algorithm.
 */
class AggregationTest extends PrivacyDataTest {

  "An Aggregation" should "aggregate tuples with AVG info" in
    confAggr(1, AggregationMethod.AVG)

  it should "aggregate tuples with MIN info" in
    confAggr(1, AggregationMethod.MIN)

  it should "aggregate tuples with MAX info" in
    confAggr(1, AggregationMethod.MAX)

  it should "aggregate tuples with STD info" in
    confAggr(8, AggregationMethod.STD)

  it should "aggregate tuples with COUNT info" in
    confAggr(8, AggregationMethod.COUNT)

  private def confAggr(col: Int, method: AggregationMethod) = {
    val aggrInfo = AggregationInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addSelectedColumnId(col)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))
//      .setSelectedColumnId(randomNumber)
      .setMethod(method)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.AggregationOperator")
      .setAggregation(aggrInfo)
      .build

    val result = AggregationOperator(operator).operate(data)
  }

}
