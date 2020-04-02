package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, NumericType}
import org.apache.spark.sql.functions.udf

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification
import ksb.csle.didentification.utilities.{FrequencyManager, StatisticManager, OutlierManager}

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the partial aggregation module in the
 * Aggregation algorithm. It discriminates outliers (currently,
 * boxplot, z-score methods are supported) and then only replaces
 * them with statistic information (e.x., min, max, avg, std, and count).
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.PartialAggrInfo]]
 *          PartialAggrInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the partial-aggregation function
 *          - method: the method to aggregate the given columns
 *          - outlierMethod: the method to discriminate the outlier
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==PartialAggrInfo==
 *  {{{
 *  enum AggregationMethod {
 *    MIN = 0;
 *    AVG = 1;
 *    MAX = 2;
 *    STD = 3;
 *    COUNT = 4;
 *    MANUAL = 5;
 *  }
 *  enum OutlierMethod {
 *    ZSCORE = 0;
 *    BOXPLOT = 1;
 *  }
 *  message PartialAggrInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required AggregationMethod method = 2 [default = AVG];
 *    required OutlierMethod outlierMethod = 3 [default = ZSCORE];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class PartialAggregateOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getPartialAggr.getCheck) {

  val p: PartialAggrInfo = o.getPartialAggr

  /**
   * Discriminates outliers among the records in the column of src dataframe
   * and then replaces them with stat. info.
   *
   * @param src Dataframe to encrypt
   * @param columnName Column to be encrypted
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    src.schema(columnName).dataType match {
      case n: NumericType =>
        anonymizeNumericColumn(src, columnName, p.getMethod, p.getOutlierMethod)
      case s: StringType =>
        anonymizeStringColumn(src, columnName, p.getOutlierMethod)
    }
  }

  /**
   * Discriminates outliers in a column containing numerical data
   * and then only replaces them with stat. info.
   *
   * @param src Dataframe to encrypt
   * @param columnName Column to be encrypted
   * @param aggrType Methods of statistic, e.x., min, max, avg, std, and count
   * @param outlierType outlier method such as z-score and boxplot
   * @return DataFrame Anonymized dataframe
   */
  def anonymizeNumericColumn(
      src: DataFrame,
      columnName: String,
      aggrType: AggregationMethod,
      outlierType: OutlierMethod): DataFrame = {
    val table = outlierType match {
      case OutlierMethod.BOXPLOT =>
        OutlierManager.makeOutlierMgmtTableBoxplot(src, columnName, aggrType)
      case OutlierMethod.ZSCORE =>
        OutlierManager.makeOutlierMgmtTableZscore(src, columnName, aggrType)
    }

    def aggrAnonymize: (String => String) = (
      key => table.get(StatisticManager.getKey(table, key)) match {
        case Some(value) =>
          if(key.toDouble < value.lower || key.toDouble > value.upper) value.replace
          else key
        case _ => "None"
      })

    val avgUdf = udf(aggrAnonymize)
    src.withColumn(columnName, avgUdf(src.col(columnName)))
  }

  /**
   * Discriminates outliers in a column containing text data
   * and then replaces them with the string with maximum frequency.
   *
   * @param src Dataframe to encrypt
   * @param columnName Column to be encrypted
   * @param outlierType outlier method such as z-score and boxplot
   * @return DataFrame Anonymized dataframe
   */
  def anonymizeStringColumn(
      src: DataFrame,
      columnName: String,
      outlierType: OutlierMethod): DataFrame = {
    val table = outlierType match {
      case OutlierMethod.BOXPLOT =>
        FrequencyManager.makeBoxplotOutlierBasedFrequency(src, columnName)
      case OutlierMethod.ZSCORE =>
        FrequencyManager.makeZscoreOutlierBasedFrequency(src, columnName)
    }

    val highestEntry = FrequencyManager.
      getHighestFrequencyEntry(src, columnName).toString
    println(highestEntry + table)
    def aggrAnonymize: (String => String) = (
      cell => table.get(cell) match {
        case Some(value) => highestEntry
        case _ => cell
      })

    val avgUdf = udf(aggrAnonymize)
    src.withColumn(columnName, avgUdf(src.col(columnName)))
  }

  /**
   * Operates partial aggregation module for basic de-identification
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

object PartialAggregateOperator {
  def apply(o: StreamOperatorInfo): PartialAggregateOperator = new PartialAggregateOperator(o)
}
