package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, NumericType}

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the random rounding module in the Data Suppression
 * algorithm. Compared to rounding module in Algorithm algorithm which is only
 * applicable on numerical values, it can be applied on variables containing
 * both numerical and string values.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.RandomRoundingInfo]]
 *          RandomRoundingInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the random rounding function
 *          - roundStep: round step
 *          - method: round up, round down, or round
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==RandomRoundingInfo==
 *  {{{
 *  enum RoundingMethod {
 *    ROUND = 0;
 *    ROUND_UP = 1;
 *    ROUND_DOWN = 2;
 *  }
 *  message RandomRoundingInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required int32 roundStep = 2 [default = 10];
 *    required RoundingMethod method = 3 [default = ROUND];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class RandomRoundingOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getRandomRounding.getCheck) {

  val p: RandomRoundingInfo = o.getRandomRounding

  /**
   * Rounds the value of column in src dataframe w.r.t. 'roundType' method
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to apply rounding
   * @param roundType Method of rounding modules (ex., round, round-up, round-down)
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    src.schema(columnName).dataType match {
      case n: NumericType =>
        RoundingOperator(o).roundingNumericColumn(src, columnName, p.getRoundStep, p.getMethod)
      case s: StringType =>
        roundingStringColumn(src, columnName, p.getMethod)
    }
  }

  /**
   * Some columns may contain both numerical and non-numerical
   * values simultaneously, i.e., 21K, \$45, etc. In this case, this
   * function extracts the numerical parts, and then only replaces them
   * with calculated statistic info defined by 'roundType' method.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to apply rounding
   * @param roundType Method of rounding modules (ex., round, round-up, round-down)
   * @return DataFrame Anonymized dataframe
   */
  def roundingStringColumn[T](
      src: DataFrame,
      columnName: String,
      roundType: T): DataFrame =
        roundingStringColumn(src, columnName, 10, roundType)

  /**
   * Same as roundingStringColumn(src, columnName, roundType).
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to apply rounding
   * @param tick Unit of rounding, initial value is 10.
   * @param roundType Method of rounding modules (ex., round, round-up, round-down)
   * @return DataFrame Anonymized dataframe
   */
  def roundingStringColumn[T](
      src: DataFrame,
      columnName: String,
      tick: Int,
      roundType: T): DataFrame = {
    require(tick != 0)

    val reg = "([^0-9]+)([0-9]+)([^0-9]+)".r
    def roundingAnonymize: (String => String) = (
      key => key match {
        case reg(a, numeric, c) =>
          a + getRoundingValue(numeric, tick, roundType).toDouble + c
      }
    )

    val roundingUdf = udf(roundingAnonymize)
    src.withColumn(columnName, roundingUdf(src.col(columnName)))
  }

  private def getRoundingValue[T](
      value: String, tick:Int, roundType: T): Int = {
    roundType match {
      case RoundingMethod.ROUND_UP =>
        if(value.toInt % tick == 0) value.toInt
        else value.toInt - (value.toInt % tick - tick)
      case RoundingMethod.ROUND_DOWN =>
        value.toInt - (value.toInt % tick)
      case RoundingMethod.ROUND =>
        if(value.toInt % tick >= tick / 2) value.toInt - (value.toInt % tick - tick)
        else value.toInt - (value.toInt % tick)
      case _ => value.toInt - (value.toInt % tick)
    }
  }

  /**
   * Operates random rounding module for basic de-identification
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

object RandomRoundingOperator {
  def apply(o: StreamOperatorInfo): RandomRoundingOperator = new RandomRoundingOperator(o)
}
