package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Data Range model
 */
class DataRangeTest extends PrivacyDataTest {

  "A DataRange" should "represent tuples of numeric column with range 5" in
    confDataRange(1, 5, AggregationMethod.AVG)

  "A DataRange" should "represent tuples of numeric column with range 10" in
    confDataRange(1, 10, AggregationMethod.AVG)

  it should "represent tuples with range info" in
    confDataRange(8, 10, AggregationMethod.AVG)

  private def confDataRange(colId: Int, step: Int, method: AggregationMethod) {
    val rangeInfo = DataRangeInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))      .addSelectedColumnId(colId)
      .setMethod(AggregationMethod.AVG)
      .setRangeStep(step)
//      .setSelectedColumnId(randomNumber)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.DataRange")
      .setDataRange(rangeInfo)
      .build

    val result = DataRangeOperator(operator).operate(data)
  }

}
