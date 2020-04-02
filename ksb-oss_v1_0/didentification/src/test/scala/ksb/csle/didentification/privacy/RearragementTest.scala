package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Iden-Partial Reduction model
 */
class RearrangementTest extends PrivacyDataTest {

  "A Rearrangement" should "rearrange records of specified columns simultaneously" in
    confRearrangement(List(0, 1, 2), 0.001, ColumnHandlePolicy.ALL, List())

  it should "rearrange records of specified columns one-by-one" in
    confRearrangement(List(0, 1, 2), 0.001, ColumnHandlePolicy.ONEBYONE, List())

  it should "rearrange specified records of specified columns simultaneously" in
    confRearrangement(List(0, 1, 2), 0.01, ColumnHandlePolicy.ALL,
        List((1, 5), (10, 22)))

  it should "rearrange specified records of specified columns one-by-one" in
    confRearrangement(List(0, 1, 2), 0.01, ColumnHandlePolicy.ONEBYONE,
        List((1, 5), (10, 22)))

  private def confRearrangement(
      colnumIDs: List[Int],
      ratio: Double,
      policy: ColumnHandlePolicy,
      swapList: List[(Int, Int)]) = {
    val rearrangeInfo = RearrangementInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))       .setColumnHandlePolicy(policy)
      .setRatio(ratio)

    if(swapList.length != 0) {
      rearrangeInfo.setMethod(RearrangeMethod.REARR_MANUAL)
      swapList.map(x => rearrangeInfo.addSwapList(
          SwapList.newBuilder()
          .setFromRowId(x._1)
          .setToRowId(x._2)))
    } else {
      rearrangeInfo.setMethod(RearrangeMethod.REARR_RANDOM)
    }

    colnumIDs.map(x => rearrangeInfo.addSelectedColumnId(x))

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.Rearrangement")
      .setRearrange(rearrangeInfo)
      .build

    val resultDF = RearrangeOperator(operator).operate(data)
  }

}
