package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.verification.Verification

import ksb.csle.didentification.exceptions.AnonymityExceptionHandler

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the quasi-identifier reduction module in the
 * Data Reduction algorithm.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.QuasiReductionInfo]]
 *          QuasiReductionInfo contains attributes as follows:
 *          - method: not yet used
 *          - safeHarborList: the list of safe habor string manually given by user
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==QuasiReductionInfo==
 *  {{{
 *  enum ReductionMethod {
 *    DELETE = 0;
 *    REPLACE = 1;
 *  }
 *  message QuasiReductionInfo {
 *    required ReductionMethod method = 1 [default = DELETE];
 *    repeated string safeHarborList = 2;
 *    repeated FieldInfo fieldInfo = 3;
 *    optional PrivacyCheckInfo check = 4;
 *  }
 *  }}}
 */
class QuasiReductionOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getQuasiReduction.getCheck) {

  val p: QuasiReductionInfo = o.getQuasiReduction

  /**
   * Anonymizes the src dataframe. Since this method is only applicable
   * on the quasi-identifier columns,
   * there is no need to specify the columns to be anonymized.
   *
   * @param src Dataframe to anonymize
   * @return DataFrame Anonymized dataframe which replaces original column
   * with anonymized column
   */
  def anonymize(src: DataFrame): DataFrame = {
    var result = src
    p.getFieldInfoList.toArray.map(col =>
      col.asInstanceOf[FieldInfo]) map (colDesc => 
        if(colDesc.getAttrType == FieldInfo.AttrType.QUASIIDENTIFIER)
          result = AnonymityExceptionHandler(result) { result =>
            anonymizeColumn(result, getColumnName(src, colDesc.getKey.toInt))
          }
      )

    (result)
  }

  /**
   * Drops the quasi-identifier column which belongs to the list of safe habor.
   * This list can be configured by the data handler, or by referring to
   * the HIPAA act.
   *
   * @param src Dataframe to anonymize
   * @param columnName Columnname  to be anonymized
   * @return DataFrame Anonymized dataframe
   */
  def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    if(isSafeBarborList(columnName)) src.drop(columnName).cache
    else if(isTypedSafeBarborList(columnName)) src.drop(columnName).cache
    else src
  }

  /**
   * Checks whether the given column belongs to the list of safe habor
   *
   * @param columnName the column name to check
   * @return Boolean returns whether belongs to the list of safe harbor or not.
   */
  def isSafeBarborList(columnName: String): Boolean = {
    getSafeHarborList.map(col => {
      if(strContains(col, columnName)) return true
    })

    return false
  }

  /**
   * Data handle can configure the list of safe habor. In the case,
   * this function checks the given column belongs to the configured list.
   *
   * @param columnName the column name to check
   * @return Boolean returns whether belongs to the list of safe harbor or not.
   */
  def isTypedSafeBarborList(columnName: String): Boolean = {
    p.getSafeHarborListList.map(col => {
      if(strContains(col, columnName)) return true
    })

    return false
  }

  private def strContains(str1: String, str2: String): Boolean = {
    if(str1.toLowerCase contains str2.toLowerCase) return true
    else if(str2.toLowerCase contains str1.toLowerCase) return true
    else return false
  }

  /**
   * get the name of safe habour list
   *
   * @param src Dataframe to anonymize
   * @return Array[String] the list of safe harbor
   */
  private def getSafeHarborList(): Array[String] = {
    Array("Name", "ZIP", "Date", "Telephone", "Fax", "E-mail",
        "Social Security Number", "Medical Record Number", "Account",
        "Licence", "Vehicle Identifiers", "Device Identifiers",
        "Web URLs", "IP Address", "Biometric Identifiers", "Photos")
  }

  /**
   * Operates quasi-reduction module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    df.show
    val result = anonymize(df)
    result.show
    // the columns belonging to safe habour may be deleted, and hence
    // it is not possible to calculate the performance result
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos)    

    (result)
  }

}

object QuasiReductionOperator {
  def apply(o: StreamOperatorInfo): QuasiReductionOperator = new QuasiReductionOperator(o)
}
