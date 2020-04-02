package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Blank and Impute model
 */
class BlankImputeTest extends PrivacyDataTest {

  "A BlankImpute" should "replace tuples (col: 1, position: 1, len: 1, replace: )" in
    confBlankImpute(1, List(1), 1, ReplaceValueMethod.BLANK)

  it should "replace tuples (col: 3, position: 2, len: 5, replace: *)" in
    confBlankImpute(3, List(2), 5, ReplaceValueMethod.STAR)

  it should "replace tuples (col: 3, position: 3, len: 2, replace: _)" in
    confBlankImpute(3, List(3), 2, ReplaceValueMethod.UNDERBAR)

  it should "replace tuples (col: 3, position: (1, 5, 7), len: 1, replace: _)" in
    confBlankImpute(3, List(1, 5, 7), 1, ReplaceValueMethod.UNDERBAR)

  private def confBlankImpute(colId: Int,
      position: List[Int], len: Int, method: ReplaceValueMethod) = {
    val blankImputeInfo = BlankImputeInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))      .addSelectedColumnId(colId)
      .setNumReplace(len)
      .setMethod(method)

    position.map(p => blankImputeInfo.addPosition(p))

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.BlankImpute")
      .setBlankImpute(blankImputeInfo)
      .build

    val resultDF = BlankImputeOperator(operator).operate(data)
  }

}
