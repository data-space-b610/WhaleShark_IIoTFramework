package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.verification.Verification

/**
 *  :: ApplicationDeveloperApi ::
 * 
 * Operator that implements the 'blank and impute' module in the Data Reduction
 * algorithm. It changes the values of the data with ' ', '*', or '_'.
 * 
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.BlankImputeInfo]]
 *          BlankImputeInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the hiding function
 *          - position: the position to replace the data
 *          - numReplace: the number of characters to replace
 *          - method: the special character to replace 
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *      
 *  ==BlankImputeInfo==
 *  {{{
 *  enum ReplaceValueMethod {
 *    BLANK = 0;
 *    STAR = 1;
 *    UNDERBAR = 2;
 *  }
 *  message BlankImputeInfo {
 *    repeated int32 selectedColumnId = 1;
 *    repeated int32 position = 2;
 *    optional int32 numReplace = 3 [default = 1];
 *    required ReplaceValueMethod method = 4 [default = STAR];
 *    repeated FieldInfo fieldInfo = 5;
 *    optional PrivacyCheckInfo check = 6;
 *  }
 *  }}}          
 */
class BlankImputeOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getBlankImpute.getCheck) {

  val p: BlankImputeInfo = o.getBlankImpute

  /**
   * Replaces the specified multiple positions of values in the columm
   * with imputeType (blank ' ', star '*', or impute '_')
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    val imputeLists: Array[Int] =
      (p.getPositionList map (_.asInstanceOf[Int])).toArray

    var result = src
    imputeLists.map(imputePosition => result = anonymizeColumn(
      result, columnName, imputePosition, p.getNumReplace, p.getMethod))

    (result)
  }

  /**
   * Replaces the specific index position of values in the columm
   * with imputeType (blank ' ', star '*', or impute '_')
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param position Position of value to be imputed
   * @param numReplace The number of replace
   * @param imputeType Blank, star, or impute
   * @return DataFrame Anonymized dataframe
   */
  def anonymizeColumn[T](
      src: DataFrame,
      columnName: String,
      position: Int,
      numReplace: Int,
      imputeType: T): DataFrame = {
    import scala.collection.mutable.StringBuilder

    def blankImpute: (String => String) = value => {
      val num = {
        if(numReplace == 0 || value.length - position < numReplace)
          value.length - position
        else numReplace
      }
      value.substring(0,position) +
      getBlankImputeStr(num, imputeType) +
      value.substring(num+position)
    }
    val blankImputeUdf = udf(blankImpute)
    src.withColumn(columnName, blankImputeUdf(src.col(columnName)))
  }

  private def getBlankImputeStr[T](lenStr: Int, imputeType: T): String = {
    var str = imputeType match {
      case ReplaceValueMethod.BLANK => " "
      case ReplaceValueMethod.STAR => "*"
      case ReplaceValueMethod.UNDERBAR => "_"
      case _ => "*"
    }
    var result = str
    for(num <- 1 until lenStr) result = result.concat(str)

    result
  }

  /**
   * Operates blank and impute module for basic de-identification
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

object BlankImputeOperator {
  def apply(o: StreamOperatorInfo): BlankImputeOperator = new BlankImputeOperator(o)
}
