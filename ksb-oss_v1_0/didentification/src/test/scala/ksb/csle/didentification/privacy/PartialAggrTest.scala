package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Partial Aggregation model
 */
class PartialAggrTest extends PrivacyDataTest {

  "A PartialAggr" should "aggregate tuples of numerical column with AVG info using Boxplot" in
    confPartialAggr(1, AggregationMethod.AVG, OutlierMethod.BOXPLOT)

  it should "aggregate tuples of numerical column with STD info using boxplot" in
    confPartialAggr(1, AggregationMethod.STD, OutlierMethod.BOXPLOT)

  it should "aggregate tuples of numerical column with AVG info using zscore" in
    confPartialAggr(1, AggregationMethod.AVG, OutlierMethod.ZSCORE)

  it should "aggregate tuples of numerical column with STD info using zscore" in
    confPartialAggr(1, AggregationMethod.STD, OutlierMethod.ZSCORE)

  it should "aggregate tuples of string column with AVG info using boxplot" in
    confPartialAggr(4, AggregationMethod.AVG, OutlierMethod.BOXPLOT)

  it should "aggregate tuples of string column with AVG info using zscore" in
    confPartialAggr(4, AggregationMethod.AVG, OutlierMethod.ZSCORE)

  private def confPartialAggr(
      colId: Int,
      method: AggregationMethod,
      outlier: OutlierMethod) = {
    val aggrInfo = PartialAggrInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))
      .addSelectedColumnId(colId)
      .setMethod(method)
      .setOutlierMethod(outlier)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.PartialAggr")
      .setPartialAggr(aggrInfo)
      .build

    val result = PartialAggregateOperator(operator).operate(data)
  }

}
