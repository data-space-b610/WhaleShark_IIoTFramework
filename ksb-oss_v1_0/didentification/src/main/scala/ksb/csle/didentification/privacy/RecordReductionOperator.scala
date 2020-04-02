package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{NumericType, StringType}

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.utilities._
import ksb.csle.didentification.exceptions.AnonymityExceptionHandler
import ksb.csle.didentification.verification.Verification

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the record reduction module in the Data
 * Reduction algorithm. It discriminates outliers (boxplot and z-score
 * methods are supported) and then replaces the rows which contains
 * these found outliers with blank (or star).
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.RecordReductionInfo]]
 *          RecordReductionInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the record reduction function
 *          - method: the special character to replace the found outlier data 
 *          - columnHandlePolicy: consider the given columns all together, or individually.
 *          - outlierMethod: the method to discriminate the outlier
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the anonymized data
 *
 *  ==RecordReductionInfo==
 *  {{{
 *  enum ReplaceValueMethod {
 *    BLANK = 0;
 *    STAR = 1;
 *    UNDERBAR = 2;
 *  }
 *  enum ColumnHandlePolicy {
 *    ONEBYONE = 0;
 *    ALL = 1;
 *  }
 *  enum OutlierMethod {
 *    ZSCORE = 0;
 *    BOXPLOT = 1;
 *  }
 *  message RecordReductionInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required ReplaceValueMethod method = 2 [default = STAR];
 *    required ColumnHandlePolicy columnHandlePolicy = 3 [default = ONEBYONE];
 *    required OutlierMethod outlierMethod = 4 [default = ZSCORE];
 *    repeated FieldInfo fieldInfo = 5;
 *    optional PrivacyCheckInfo check = 6;
 *  }
 *  }}}
 */
class RecordReductionOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getRecordReduction.getCheck) {

  val p: RecordReductionInfo = o.getRecordReduction

  /**
   * Anonymizes the src dataframe
   *
   * @param src Dataframe to anonymize
   * @return DataFrame Anonymized dataframe
   */
  // getQuasiColumnNames may be defined for the data reliability for future
  def anonymize(src: DataFrame): DataFrame = {
    var result = src
    p.getColumnHandlePolicy match {
      case ColumnHandlePolicy.ONEBYONE =>
        result = anonymizedOneByOne(src, getColumnName(src))
      case ColumnHandlePolicy.ALL =>
        result = anonymizedAll(src, getColumnName(src))
    }

    (result)
  }

  // currently we used this private function, but it may be deleted..
  private def getColumnName(src: DataFrame): Array[String] = {
    var columnNames = Array[String]()

    p.getFieldInfoList.toArray.map(col =>
      col.asInstanceOf[FieldInfo]) map (colDesc => {
        if(colDesc.getAttrType == FieldInfo.AttrType.QUASIIDENTIFIER)
          columnNames = columnNames :+ getColumnName(src, colDesc.getKey.toInt)
      })

    columnNames
  }

  /**
   * Performs the record reduction on the given array of columns one by one.
   *
   * @param src Dataframe to anonymize
   * @return DataFrame Anonymized dataframe
   */
  def anonymizedOneByOne(src: DataFrame, columnNames: Array[String]): DataFrame = {
    var result = src
    columnNames.map(col => result =
      AnonymityExceptionHandler(result) {
        result => anonymizeColumn(result, col)
    })
    (result)
  }

  /**
   * Performs the record reduction on column of src dataframe.
   *
   * @param src Dataframe to anonymize
   * @param columnName the column of dataframe to apply record reduction
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    src.schema(columnName).dataType match {
      case n: NumericType =>
        anonymizeNumericColumn(src, columnName, p.getMethod, p.getOutlierMethod)
      case s: StringType =>
        anonymizeStringColumn(src, columnName, p.getMethod, p.getOutlierMethod)
    }
  }

  /**
   * Performs the record reduction on column which has the type of numerical
   * values. It discriminates outliers on the basis of the statistical information,
   * using given outlier method, and replaces them with 'repValueType'.
   *
   * @param src Dataframe to anonymize
   * @param columnName the (numerical) column of dataframe to apply record reduction
   * @param repValueType The value of replacing outliers (e.x., blank, _, and *)
   * @param outlierType the method to discrimiate outliers (e.x., boxplot, z-score)
   * @return DataFrame Anonymized dataframe
   */
  def anonymizeNumericColumn(
      src: DataFrame,
      columnName: String,
      repValueType: ReplaceValueMethod,
      outlierType: OutlierMethod): DataFrame = {
    val table = outlierType match {
      case OutlierMethod.BOXPLOT => OutlierManager.
        makeOutlierMgmtTableBoxplot(src, columnName, repValueType, 1)
      case OutlierMethod.ZSCORE => OutlierManager.
        makeOutlierMgmtTableZscore(src, columnName, repValueType, 1)
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
   * Performs the record reduction on column which has the type of string
   * values. It discriminates outliers on the basis of their frequencys,
   * using given outlier method, and replaces them with 'repValueType'.
   *
   * @param src Dataframe to anonymize
   * @param columnName the (numerical) column of dataframe to apply record reduction
   * @param repValueType The value of replacing outliers (e.x., blank, _, and *)
   * @param outlierType the method to discrimiate outliers (e.x., boxplot, z-score)
   * @return DataFrame Anonymized dataframe
   */
  def anonymizeStringColumn(
      src: DataFrame,
      columnName: String): DataFrame =
        anonymizeStringColumn(
            src, columnName, ReplaceValueMethod.STAR, OutlierMethod.ZSCORE)

  def anonymizeStringColumn(
      src: DataFrame,
      columnName: String,
      repValueType: ReplaceValueMethod,
      outlierType: OutlierMethod): DataFrame = {
    val table = outlierType match {
      case OutlierMethod.BOXPLOT => FrequencyManager.
        makeBoxplotOutlierBasedFrequency(src, columnName)
      case OutlierMethod.ZSCORE => FrequencyManager.
        makeZscoreOutlierBasedFrequency(src, columnName)
    }

    def aggrAnonymize: (String => String) = (
      cell => table.get(cell) match {
        case Some(value) => repValueType match {
          case ReplaceValueMethod.STAR => "*"
          case ReplaceValueMethod.BLANK => " "
          case ReplaceValueMethod.UNDERBAR => "_"
          case _ => "*"
        }
        case _ => cell
      })

    val avgUdf = udf(aggrAnonymize)
    src.withColumn(columnName, avgUdf(src.col(columnName)))
  }

  /**
   * Performs the record reduction on the given array of columns simultaneously.
   * This function discriminates outliers by considering the given array of
   * column simultaneously, and then replace them together.
   *
   * @param src Dataframe to anonymize
   * @return DataFrame Anonymized dataframe
   */
  def anonymizedAll(src: DataFrame, columnNames: Array[String]): DataFrame = {
    val table = p.getOutlierMethod match {
      case OutlierMethod.BOXPLOT => FrequencyManager.
        makeBoxplotOutlierBasedFrequency(src, columnNames)
      case OutlierMethod.ZSCORE => FrequencyManager.
        makeZscoreOutlierBasedFrequency(src, columnNames)
    }

    SuppressingManager.suppressingDataFrame(src, columnNames, table)
  }

  /**
   * Operates record reduction module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    df.show
    val result = anonymize(df)
    result.show
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos)    

    (result)
  }

}

object RecordReductionOperator {
  def apply(o: StreamOperatorInfo): RecordReductionOperator = new RecordReductionOperator(o)
}
