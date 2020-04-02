package ksb.csle.didentification.privacy

import org.apache.spark.sql.DataFrame

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto.PrivacyCheckInfo

import ksb.csle.didentification.interfaces.DataFrameCheck
import ksb.csle.didentification.exceptions.AnonymityExceptionHandler
import ksb.csle.didentification.verification.Verification

/**
 * This abstract class defines some functions that are shared
 * by basic de-identification modules.
 */
abstract class BasePrivacyAnonymizer(o: StreamOperatorInfo, p: PrivacyCheckInfo)
  extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with DataFrameCheck {

  val privacy: PrivacyCheckInfo = p

  /**
   * Anonymizes the column specified in src dataframe
   * using generic 'Type' method. The 'Type' is decided by
   * inherited object module.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param anonymizeType Methods of anonymization module
   * @return DataFrame The dataframe which replaces original column
   * with anonymized column
   */
  def anonymize(
      src: DataFrame,
      columnName: String): DataFrame = {
    val result = AnonymityExceptionHandler(src) { src =>
            anonymizeColumn(src, columnName) }

    (result)
  }

  def anonymize(
      src: DataFrame,
      columnNames: Array[String]): DataFrame = {
    var result = src
    columnNames.map(column => result = anonymize(result, column))

//    val fieldInfos = (privacy.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
//    logger.info(src.show.toString())
//    logger.info(result.show.toString())

    (result)
  }

  /**
   * This abstract function defines how to anonymize the column.
   * The object inheriting this abstract class should override this function.
   *
   * @param src dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param anonymizeType Methods of anonymization module
   * @return DataFrame The dataframe which replaces original column
   * with anonymized column
   */
  def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame

}
