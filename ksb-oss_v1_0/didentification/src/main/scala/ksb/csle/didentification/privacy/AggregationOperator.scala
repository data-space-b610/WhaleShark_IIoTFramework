package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{NumericType, StringType}

import ksb.csle.didentification.utilities.StatisticManager
import ksb.csle.didentification.verification.Verification

import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamDidentProto.AggregationInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 *  :: ApplicationDeveloperApi ::
 * 
 * Operator that implements the aggregation module in the Aggregation
 * algorithm. It replaces the values of the data with some
 * statistic values such as min, max, avg, std, or count.
 * If the type of record is string containing numerical value, this module
 * extracts only numerical value and then applies this function on it.
 * 
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.AggregationInfo]]
 *          AggregationInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the aggregation function
 *          - method: the method to aggregate the given columns
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *      
 *  ==AggregationInfo==
 *  {{{
 *  enum AggregationMethod {
 *    MIN = 0;
 *    AVG = 1;
 *    MAX = 2;
 *    STD = 3;
 *    COUNT = 4;
 *    MANUAL = 5;
 *  }
 *  message AggregationInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required AggregationMethod method = 2 [default = AVG];
 *    repeated FieldInfo fieldInfo = 3;
 *    optional PrivacyCheckInfo check = 4;
 *  }
 *  }}}          
 */
class AggregationOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getAggregation.getCheck) {

  val p: AggregationInfo = o.getAggregation

  /**
   * Replaces the values of the column with statistic information
   * using 'aggrType' method such as min, max, avg, std, and count.
   * If the type of column is string, this module call the hiding
   * module internally.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param aggrType Methods of aggregation module. ex., min, max, avg, std, count
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    src.schema(columnName).dataType match {
      case n: NumericType =>
        aggregateNumericColumn(src, columnName, p.getMethod)
      case s: StringType =>
        HidingOperator(o).hidingStringColumn(src, columnName, p.getMethod)
    }
  }

  /**
   * Replaces the values of the column containing numerical data with statistic
   * information using 'aggrType' method such as min, max, avg, std, and count
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param aggrType Methods of aggregation module. ex., min, max, avg, std, and count
   * @return DataFrame Anonymized dataframe
   */
  def aggregateNumericColumn[T](
      src: DataFrame,
      columnName: String,
      aggrType: T): DataFrame = {
//    val table = StatisticManager.makeAgeStatTable(src, columnName, aggrType)
    val table = StatisticManager.makeNumericStatTable(src, columnName, aggrType)
    def aggrAnonymize: (String => String) = (
      key => table.get(StatisticManager.getKey(table, key)) match {
        case Some(value) => value
        case _ => "0.0"
      })

    val avgUdf = udf(aggrAnonymize)
    src.withColumn(columnName, avgUdf(src.col(columnName)))
  }

  /**
   * Operates aggregation module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    df.show
    val columnIDs: Array[Int] =
      (p.getSelectedColumnIdList map (_.asInstanceOf[Int])).toArray
    val columnNames = getColumnNames(df, columnIDs)
    val result = anonymize(df, columnNames)
    
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos)

    (result)
  }

}

object AggregationOperator {
  def apply(o: StreamOperatorInfo): AggregationOperator = new AggregationOperator(o)
}
