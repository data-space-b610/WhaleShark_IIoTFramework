package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Record Reduction model
 */
class RecordReductionTest extends PrivacyDataTest {

  "A RecordReduction" should "deletes records with of columns using boxplot" in
    confRecordReduction(List("1", "3"), OutlierMethod.BOXPLOT,
        ReplaceValueMethod.BLANK, ColumnHandlePolicy.ONEBYONE)

  it should "deletes records with of columns using zscore one-by-one" in
    confRecordReduction(List("1", "3"), OutlierMethod.ZSCORE,
        ReplaceValueMethod.STAR, ColumnHandlePolicy.ONEBYONE)

  it should "deletes records with of columns using boxplot simultaneously" in
    confRecordReduction(List("1", "3"), OutlierMethod.BOXPLOT,
        ReplaceValueMethod.BLANK, ColumnHandlePolicy.ALL)

  it should "deletes records with of columns using zscore simultaneously" in
    confRecordReduction(List("1", "3"), OutlierMethod.ZSCORE,
        ReplaceValueMethod.BLANK, ColumnHandlePolicy.ALL)

  private def confRecordReduction(
      colnumIDs: List[String],
      method: OutlierMethod,
      replaceMethod: ReplaceValueMethod,
      policy: ColumnHandlePolicy) = {
    val reductionInfo = RecordReductionInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))       .addSelectedColumnId(1)
      .setMethod(replaceMethod)
      .setOutlierMethod(method)
      .setColumnHandlePolicy(policy)

    colnumIDs.map(col => {
      reductionInfo.addFieldInfo(
        FieldInfo.newBuilder()
          .setKey(col)
          .setType(FieldInfo.FieldType.STRING)
          .setAttrType(FieldInfo.AttrType.QUASIIDENTIFIER)
          .build()
      )
    })

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.RecordReduction")
      .setRecordReduction(reductionInfo)
      .build

    val resultDF = RecordReductionOperator(operator).operate(data)
  }

}
