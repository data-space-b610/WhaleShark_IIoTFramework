package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{NumericType, StringType}

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the Rounding module in the Aggregation
 * algorithm. It rounds up or down given numerical values.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.RoundingInfo]]
 *          RoundingInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the rounding function
 *          - roundStep: the step of round (ex., teenage for age)
 *          - method: how to round the given data
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==RoundingInfo==
 *  {{{
 *  enum RoundingMethod {
 *    ROUND = 0;
 *    ROUND_UP = 1;
 *    ROUND_DOWN = 2;
 *  }
 *  message RoundingInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required int32 roundStep = 2 [default = 10];
 *    required RoundingMethod method = 3 [default = ROUND];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class RoundingOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getRounding.getCheck) {

  val p: RoundingInfo = o.getRounding

  /**
   * Rounds the values of the column in src dataframe
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
        roundingNumericColumn(src, columnName, p.getRoundStep, p.getMethod)
      case s: StringType =>
        RandomRoundingOperator(o).anonymizeColumn(src, columnName)
    }
  }

  /**
   * Rounds the numercial values of the column in src dataframe
   * using 'roundType' method
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param tick the unit of round
   * @param roundType Methods of rounding modules.
   * ex., round, round-up, round-down
   * @return DataFrame Anonymized dataframe
   */
  def roundingNumericColumn[T](
      src: DataFrame,
      columnName: String,
      roundType: T): DataFrame =
        roundingNumericColumn(src, columnName, 10, roundType)

  def roundingNumericColumn[T](
      src: DataFrame,
      columnName: String,
      tick: Int,
      roundType: T): DataFrame = {
    require(tick != 0)

    def roundingAnonymize: (String => Int) =
      value => roundType match {
      case RoundingMethod.ROUND_UP =>
        if(value.toInt % tick == 0) value.toInt
        else value.toInt - (value.toInt % tick - tick)
      case RoundingMethod.ROUND_DOWN =>
        value.toInt - (value.toInt % tick)
      case RoundingMethod.ROUND =>
        if(value.toDouble % tick >= tick.toDouble / 2)
          value.toInt - (value.toInt % tick - tick)
        else value.toInt - (value.toInt % tick)
      case _ => value.toInt - (value.toInt % tick)
    }

    val roundingUdf = udf(roundingAnonymize)
    src.withColumn(columnName, roundingUdf(src.col(columnName)))
  }

  /**
   * Operates rounding module for basic de-identification
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

object RoundingOperator {
  def apply(o: StreamOperatorInfo): RoundingOperator = new RoundingOperator(o)
}
