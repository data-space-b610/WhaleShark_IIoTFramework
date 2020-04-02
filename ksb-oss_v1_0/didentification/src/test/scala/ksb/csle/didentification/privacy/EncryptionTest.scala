package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Encryption model
 */
class EncryptionTest extends PrivacyDataTest {

  "An Encryption" should "encrypt tuples using AES with NORMAL" in
    confEncryption(EncryptionKey.NORMAL, EncryptionMethod.AES)

  it should "encrypt tuples using AES with SHA1" in
    confEncryption(EncryptionKey.SHA1, EncryptionMethod.AES)

  it should "encrypt tuples using AES with SHA512" in
    confEncryption(EncryptionKey.NORMAL, EncryptionMethod.AES)

  it should "encrypt tuples using DES with NORMAL" in
    confEncryption(EncryptionKey.NORMAL, EncryptionMethod.DES)

//  it should "encrypt tuples using DES with SHA1" in
//    confEncryption(EncryptionKey.SHA1, EncryptionMethod.DES)
//
//  it should "encrypt tuples using DES with SHA512" in
//    confEncryption(EncryptionKey.SHA512, EncryptionMethod.DES)

  private def confEncryption(key: EncryptionKey, method: EncryptionMethod) {
    val encryptionInfo = EncryptionInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))      .addSelectedColumnId(2)
      .setKey(key)
      .setMethod(method)
      .build()

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.Encryption")
      .setEncryption(encryptionInfo)
      .build

    val result = EncryptionOperator(operator).operate(data)
  }

}
