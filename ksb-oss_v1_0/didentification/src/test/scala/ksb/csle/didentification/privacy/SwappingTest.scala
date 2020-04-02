package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Heuristic model
 */
class SwappingTest extends PrivacyDataTest {

  "A Swapping" should "swaps the records of Column 1 using File Column 1" in
    confSwapping(1, "./src/main/resources/adult.csv", 2)

  "A Swapping" should "swaps the records of Column 2 using File Column 3" in
    confSwapping(2, "./src/main/resources/adult.csv", 3)

  "A Swapping" should "swaps the records of Column 3 using File Column 0" in
    confSwapping(3, "./src/main/resources/adult.csv", 0)

  private def confSwapping(
      DFColumnId: Int,
      path: String,
      fileColumnId: Int) = {
    val swappingInfo = SwappingInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))       .addSelectedColumnId(DFColumnId)
      .setMethod(GenSwappingTableMethod.SWAP_FILE)
      .setFileInfo(SwappingFileInfo.newBuilder()
          .setFilePath(path)
          .setColumnIndex(fileColumnId)
          .build)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.Swapping")
      .setSwapping(swappingInfo)
      .build

    val result = SwappingOperator(operator).operate(data)
  }

}
