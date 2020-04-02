package ksb.csle.didentification.privacy

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo.FieldType

import ksb.csle.didentification.PrivacyDataTest

/**
 * Test class for Random Noise model
 */
class RandomNoiseTest extends PrivacyDataTest {

  "A RandomNoise" should "inserts noises (Numeric, Fixed, Multiply)" in
    confRandomNoise(1, 2, RandomType.FIXED, 5.0, NoiseOperator.NOISE_MULTIPLY)

  it should "inserts noises (Numeric, Fixed, Divide)" in
    confRandomNoise(1, 2, RandomType.FIXED, 5.0, NoiseOperator.NOISE_DIVIDE)

  it should "inserts noises (Numeric, Random, Sum)" in
    confRandomNoise(1, 2, RandomType.RANDOM, 5.0, NoiseOperator.NOISE_SUM)

  it should "inserts noises (Numeric, Gaussian, Minus)" in
    confRandomNoise(1, 2, RandomType.GAUSSIAN, 5.0, NoiseOperator.NOISE_MINUS)

  it should "inserts noises (Numeric, Gaussian, Sum, 10.0)" in
    confRandomNoise(1, 2, RandomType.GAUSSIAN, 5.0, NoiseOperator.NOISE_SUM, 10.0)

  it should "inserts noises (String, Gaussian, Minus)" in
    confRandomNoise(3, 4)

  private def confRandomNoise(
      colId: Int,
      len: Int,
      randType: RandomType = RandomType.FIXED,
      value: Double = 0.0,
      oper: NoiseOperator = NoiseOperator.NOISE_SUM,
      mu: Double = 0.0) = {
    val strHandleInfo = StringHandle.newBuilder()
      .setPosition(2)
      .setLength(len)
      .setRandMethod(RandomMethod.MIXED)
      .build

    val normDist = NormalDistInfo.newBuilder()
      .setMu(mu)
      .setStd(1.0)
      .build

    val numHandleInfo = NumericHandle.newBuilder()
      .setIsRandom(randType)
      .setOperator(oper)
      .setValue(value)
      .setNormalDist(normDist)
      .build

    val noiseInfo = RandomNoiseInfo.newBuilder()
      .setCheck(getBasePrivacyInfo)
      .addFieldInfo(
          makeFieldInfo("0", FieldType.STRING, FieldInfo.AttrType.IDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("1", FieldType.INTEGER, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("2", FieldType.STRING, FieldInfo.AttrType.QUASIIDENTIFIER))
      .addFieldInfo(
          makeFieldInfo("3", FieldType.STRING, FieldInfo.AttrType.SENSITIVE))       .addSelectedColumnId(colId)
      .setStrHandle(strHandleInfo)
      .setNumHandle(numHandleInfo)
      .build

    val operator = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.didentification.privacy.RandomNoise")
      .setRandomNoise(noiseInfo)
      .build

    val result = RandomNoiseOperator(operator).operate(data)
  }

}
