package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, NumericType}

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification
import ksb.csle.didentification.utilities.{LayoutManager, RandomGenerator}

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the random noise module in the Data Masking
 * algorithm. It inserts random noises on original data.
 * - If the given column is string type, random noises composed of
 * numerical, or alphabet, or both are inserted at specific position.
 * - If the given column is numerical type, some values (it may be specified,
 * randomly chosen, or got from the normal distribution) are added
 * (or subtracted, multiplied, and divided) on each value of that column.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.RandomNoiseInfo]]
 *          RandomNoiseInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the random noise function
 *          - strHandle: how to add the noise into the string values
 *          - numHandle: how to add the noise into the numerical values
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==RandomNoiseInfo==
 *  {{{
 *  message StringHandle {
 *  	required int32 position = 1;
 *  	required int32 length = 2;
 *  	required RandomMethod randMethod = 3 [default = MIXED];
 *  }
 *  enum RandomType {
 *  	FIXED = 0;
 *  	RANDOM = 1;
 *  	GAUSSIAN = 2;
 *  }
 *  enum NoiseOperator {
 *  	NOISE_SUM = 0;
 *  	NOISE_MINUS = 1;
 *  	NOISE_MULTIPLY = 2;
 *  	NOISE_DIVIDE = 3;
 *  }
 *  message NormalDistInfo {
 *  	required double mu = 1 [default = 0.0];
 *  	required double std = 2 [default = 1.0];
 *  }
 *  message NumericHandle {
 *  	required RandomType isRandom = 1;
 *  	required NoiseOperator operator = 2 [default = NOISE_SUM];
 *  	optional double value = 3;
 *  	optional NormalDistInfo normalDist = 4;
 *  }
 *  message RandomNoiseInfo {
 *    repeated int32 selectedColumnId = 1;
 *    optional StringHandle strHandle = 2;
 *    optional NumericHandle numHandle = 3;
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class RandomNoiseOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getRandomNoise.getCheck) {

  val p: RandomNoiseInfo = o.getRandomNoise

  /**
   * Performs random noise operations on given src dataframe
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    src.schema(columnName).dataType match {
    case n: NumericType =>
      noiseNumericColumn(src, columnName, p.getNumHandle)
    case s: StringType =>
      noiseStringColumn(src, columnName)
    }
  }

  /**
   * Performs random noise operations on the given numerical column
   * in src dataframe. NumHandler contains the information about
   * how to generates noises.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param numHandle The method to generate noises
   * @return DataFrame Anonymized dataframe
   */
  def noiseNumericColumn(
      src: DataFrame,
      columnName: String,
      numHandle: NumericHandle): DataFrame = {
    def randNoiseAnonymize: (Double => Double) = (
      key => getManipulatedValue(
          key, getRandValue(numHandle), numHandle.getOperator))

    val randNoiseUdf = udf(randNoiseAnonymize)
    src.withColumn(columnName, randNoiseUdf(src.col(columnName)))
  }

  /**
   * Gets noise values. NumHandler contains the information about
   * how to generates noises. The noise can be set by user manually,
   * or chosen by random function, or chosen by referring to normal
   * distribution.
   *
   * @param numHandle The method to generate noises
   * @return Double generated noise
   */
  private def getRandValue(numHandle: NumericHandle): Double = {
    val randType = numHandle.getIsRandom
    val normalDist = numHandle.getNormalDist
    randType match {
      case RandomType.FIXED => numHandle.getValue
      case RandomType.RANDOM => math.random * numHandle.getValue // math.random => [0, 1]
      case RandomType.GAUSSIAN => scala.util.Random.nextGaussian() *
        normalDist.getStd + normalDist.getMu // nextGaussian => (mu:0, std:1)
    }
  }

  private def getManipulatedValue(
      value1: Double,
      value2: Double,
      operator: NoiseOperator): Double = {
    operator match {
      case NoiseOperator.NOISE_SUM => value1 + value2
      case NoiseOperator.NOISE_MINUS => value1 - value2
      case NoiseOperator.NOISE_MULTIPLY => value1 * value2
      case NoiseOperator.NOISE_DIVIDE => value1 / value2
    }
  }

  /**
   * Performs random noise operations on the given string column. Note that
   * this column may be composed of only string data or both numerical and
   * string data.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @return DataFrame Anonymized dataframe
   */
  def noiseStringColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    if(LayoutManager.isStringColumn(src, columnName))
      noiseStringOnlyColumn(src, columnName, p.getStrHandle)
    else noiseNumericStringColumn(src, columnName, p.getNumHandle)
  }

  /**
   * Performs random noise operations on the given string column. Note that
   * this column is composed of both numerical and string data. In this case,
   * this function extracts numerical data only, inserts noises, and then
   * combines the other string data.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param numHandle The method to generate noises
   * @return DataFrame Anonymized dataframe
   */
  def noiseNumericStringColumn(
      src: DataFrame,
      columnName: String,
      numHandle: NumericHandle): DataFrame = {
    val reg = "([^0-9]+)([0-9]+)([^0-9]+)".r
    def randNoiseAnonymize: (String => String) = (
      key => key match {
        case reg(a, numeric, c) => a + {
          getManipulatedValue(numeric.toDouble,
              getRandValue(numHandle), numHandle.getOperator)
        } + c
      }
    )

    val randNoiseUdf = udf(randNoiseAnonymize)
    src.withColumn(columnName, randNoiseUdf(src.col(columnName)))
  }

  /**
   * Performs random noise operations on the given string column. Note that
   * this column is only composed of string data. In this case,
   * random noises are inserted at specific position.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param strHandle The method to generate noises
   * @return DataFrame Anonymized dataframe
   */
  def noiseStringOnlyColumn(
      src: DataFrame,
      columnName: String,
      strHandle: StringHandle): DataFrame =
        noiseStringOnlyColumn(src, columnName, strHandle.getPosition,
            strHandle.getLength, strHandle.getRandMethod)

  /**
   * Same as noiseStringOnlyColumn(src, columeName, strHandle), but
   * the given parameter is different.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param position The position to add noises
   * @param length The length of generated noises
   * @param randMethod How to make the random string
   * @return DataFrame Anonymized dataframe
   */
  def noiseStringOnlyColumn(
      src: DataFrame,
      columnName: String,
      position: Int,
      length: Int,
      randMethod: RandomMethod): DataFrame = {
    def randNoise: (String => String) = value => {
      val (first, second) = value.splitAt(position)
      randMethod match {
        case RandomMethod.ALPHABET => first +
          RandomGenerator.randomAlpha(length) + second
        case RandomMethod.NUMBER => first +
          RandomGenerator.randomNumber(length) + second
        case RandomMethod.MIXED => first +
          RandomGenerator.randomMixed(length) + second
      }
    }
    val randNoiseUdf = udf(randNoise)
    src.withColumn(columnName, randNoiseUdf(src.col(columnName)))
  }

  /**
   * Operates random noise module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    val columnIDs: Array[Int] =
      (p.getSelectedColumnIdList map (_.asInstanceOf[Int])).toArray

    val result = anonymize(df, getColumnNames(df, columnIDs))
    
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos)
    (result)
  }

}

object RandomNoiseOperator {
  def apply(o: StreamOperatorInfo): RandomNoiseOperator = new RandomNoiseOperator(o)
}
