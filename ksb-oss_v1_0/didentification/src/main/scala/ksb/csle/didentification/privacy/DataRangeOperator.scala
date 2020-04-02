package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, NumericType}

import ksb.csle.didentification.utilities.StatisticManager
import ksb.csle.didentification.verification.Verification

import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamDidentProto.DataRangeInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the data range module in the Data Suppression
 * algorithm. It represents the values of the data with intervals [lower, upper].
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.DataRangeInfo]]
 *          DataRangeInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the data range function
 *          - rangeStep: the interval of range
 *          - method: how to replace the data included in the given range
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==DataRangeInfo==
 *  {{{
 *  enum AggregationMethod {
 *    MIN = 0;
 *    AVG = 1;
 *    MAX = 2;
 *    STD = 3;
 *    COUNT = 4;
 *    MANUAL = 5;
 *  }
 *  message DataRangeInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required int32 rangeStep = 2 [default = 10];
 *    required AggregationMethod method = 3 [default = AVG];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class DataRangeOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getDataRange.getCheck) {

  val p: DataRangeInfo = o.getDataRange

  /**
   * Represents the values of column in src dataframe with
   * a form of interval [lower, upper]
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
        rangeNumericColumn(src, columnName)
      case s: StringType =>
        rangeStringColumn(src, columnName, p.getMethod)
    }
  }

  /**
   * Represents the numerical type of column
   * as a form of intervals [lower, upper] using 'rangeType' method.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param rangeType Methods of data range module. currently, not supported
   * @return DataFrame Anonymized dataframe
   */
  def rangeNumericColumn(
      src: DataFrame,
      columnName: String): DataFrame =
        rangeNumericColumn(src, columnName, p.getMethod, p.getRangeStep)

  def rangeNumericColumn[T](
      src: DataFrame,
      columnName: String,
      rangeType: T,
      step: Int): DataFrame = {
    val table = StatisticManager.makeNumericStatTable(src, columnName, rangeType, step)
    def rangeAnonymize: (String => String) = (
      key => {
        val interval = StatisticManager.getKey(table, key)
        "[" + interval.lower.toInt + "," + interval.upper.toInt + "]"
      }
    )

    val rangeUdf = udf(rangeAnonymize)
    src.withColumn(columnName, rangeUdf(src.col(columnName)))
  }

  /**
   * Represents the values of column containing both numerical and string
   * as a form of intervals [lower, upper] using 'rangeType' method.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param rangeType Methods of data range module. currently, not supported
   */
  def rangeStringColumn[T](
      src: DataFrame,
      columnName: String,
      rangeType: T): DataFrame = {
    val table = StatisticManager.makeMixedStatTable(src, columnName, rangeType)
    val reg = "([^0-9]+)([0-9]+)([^0-9]+)".r
    def rangeAnonymize: (String => String) = (
      key => key match {
        case reg(a, numeric, c) => {
          val interval = StatisticManager.getKey(table, numeric)
          a + "[" + interval.lower.toInt + "," + interval.upper.toInt + "]" + c
        }

      }
    )

    val rangeUdf = udf(rangeAnonymize)
    src.withColumn(columnName, rangeUdf(src.col(columnName)))
  }

  /**
   * Operates data range module for basic de-identification
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

object DataRangeOperator {
  def apply(o: StreamOperatorInfo): DataRangeOperator = new DataRangeOperator(o)
}
