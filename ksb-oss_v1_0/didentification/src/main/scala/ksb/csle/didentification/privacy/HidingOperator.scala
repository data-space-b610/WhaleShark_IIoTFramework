package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, NumericType}

import ksb.csle.didentification.utilities.{StatisticManager, LayoutManager}
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the hiding module in the Data Suppression algorithm.
 * It replaces (or hides) the values of the data with some statistic values
 * such as min, max, or avg. Compared with aggregation module, which
 * is only applicable to numerical data, this module can be applied on string
 * data containing numerical values such as 20K, \$40.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.HidingInfo]]
 *          HidingInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the hiding function
 *          - method: how to hide the given data.
 *          - isDataRange: whether to replace the given data to statistic or range of interval
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==HidingInfo==
 *  {{{
 *  enum AggregationMethod {
 *    MIN = 0;
 *    AVG = 1;
 *    MAX = 2;
 *    STD = 3;
 *    COUNT = 4;
 *    MANUAL = 5;
 *  }
 *  message HidingInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required AggregationMethod method = 2 [default = AVG];
 *    optional bool isDataRange = 3 [default = false];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class HidingOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getHiding.getCheck) {

  val p: HidingInfo = o.getHiding

  /**
   * Anonymizes the column in src dataframe using 'hidingType' method.
   * - If the data range mode is on, call the DataRange module internally
   * - If the specified column is numerical type, then this function calls
   * the function in the Aggregation module internally.
   * - If the specified column is only string type, it calls the function
   * in the RecordReduction module internally.
   * - If the specified column is both numeric and string mixed, it separates
   * the numeric values and applies the hiding function only on them. And then,
   * it combines the above hiding values and separated string.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    if(p.getIsDataRange) DataRangeOperator(o).anonymizeColumn(src, columnName)
    else {
      src.schema(columnName).dataType match {
        case n: NumericType =>
          AggregationOperator(o).aggregateNumericColumn(src, columnName, p.getMethod)
        case s: StringType =>
          hidingStringColumn(src, columnName, p.getMethod)
      }
    }
  }

  /**
   * Hides the string column using 'aggrType' method.
   * The values in this column may be comprised of string only,
   * or of both numerical and string mixed.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param hidingType Methods to hide. ex., min, max, avg, and std
   */
  def hidingStringColumn[T](
      src: DataFrame,
      columnName: String,
      hidingType: T): DataFrame = {
    if(LayoutManager.isStringColumn(src, columnName))
      hidingStringColumn(src, columnName)
    else hidingNumericStringColumn(src, columnName, hidingType)
  }

  /**
   * Hides the column containing both numerical and string
   * values in src dataframe using 'aggrType' method.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param hidingType Methods to hide. ex., min, max, avg, and std
   * @return DataFrame Anonymized dataframe
   */
  def hidingNumericStringColumn[T](
      src: DataFrame,
      columnName: String,
      hidingType: T): DataFrame = {
    val table = StatisticManager.makeMixedStatTable(src, columnName, hidingType)
    val reg = "([^0-9]+)([0-9]+)([^0-9]+)".r
    def hidingAnonymize: (String => String) = (
      key => key match {
        case reg(a, numeric, c) => a + (
          table.get(StatisticManager.getKey(table, numeric)) match {
            case Some(value) => value
            case _ => 0.0
          }) + c
      }
    )

    val hidingUdf = udf(hidingAnonymize)
    src.withColumn(columnName, hidingUdf(src.col(columnName)))
  }

  /**
   * Hides the column containing only string values in src dataframe.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @return DataFrame Anonmized dataframe
   */
  // replace the outlier of column (based on the outlier of frequency) with *.
  def hidingStringColumn(
      src: DataFrame,
      columnName: String): DataFrame =
        RecordReductionOperator(o).anonymizeStringColumn(src, columnName)
//        PartialAggr(o).anonymizeStringColumn(src, columnName, OutlierMethod.ZSCORE)

  /**
   * Operates hiding module for basic de-identification
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

object HidingOperator {
  def apply(o: StreamOperatorInfo): HidingOperator = new HidingOperator(o)
}
