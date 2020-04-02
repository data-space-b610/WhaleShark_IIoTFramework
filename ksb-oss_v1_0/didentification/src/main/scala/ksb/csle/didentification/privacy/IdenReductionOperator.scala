package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.exceptions.AnonymityExceptionHandler
import ksb.csle.didentification.verification.Verification

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the identity reduction module in the
 * Data Reduction algorithm.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.IdenReductionInfo]]
 *          IdenReductionInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to delete 
 *          - method: delete the given column (the replace method is not yet built) 
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==IdenReductionInfo==
 *  {{{
 *  enum ReductionMethod {
 *    DELETE = 0;
 *    REPLACE = 1;
 *  }
 *  message IdenReductionInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required ReductionMethod method = 2 [default = DELETE];
 *    repeated FieldInfo fieldInfo = 3;
 *    optional PrivacyCheckInfo check = 4;
 *  }
 *  }}}
 */
/**
 * This object implements the identity reduction module in the
 * Data Reduction algorithm.
 */
class IdenReductionOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getIdenReduction.getCheck) {

  val p: IdenReductionInfo = o.getIdenReduction

  /**
   * Anonymizes the src dataframe. Since this method is only applicable
   * on the identifier column, there is no need to specify the columns to be anonymized.
   *
   * @param src Dataframe to anonymize
   * @return DataFrame Anonymized dataframe which replaces original column
   * with anonymized column
   */
  def anonymize(src: DataFrame): DataFrame = {
    var result = src
    p.getFieldInfoList.toArray.map(col =>
      col.asInstanceOf[FieldInfo]) map (colDesc => {
        if(colDesc.getAttrType == FieldInfo.AttrType.IDENTIFIER)
          result = AnonymityExceptionHandler(result) { result =>
            anonymizeColumn(result, getColumnName(src, colDesc.getKey.toInt))
          }
      })

    (result)
  }

  /**
   * Drops the identifier column or replaces it with randomized string
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be anonymized
   * @param reductionType Deletes the column or replace with random string
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    p.getMethod match {
      case ReductionMethod.DELETE => src.drop(columnName).cache
//      case ReductionMethod.REPLACE =>
//        Heuristic(o).anonymizeColumn(src, columnName, RandomMethod.MIXED)
      case _ => src.drop(columnName).cache
    }
  }

  /**
   * Operates iden reduction module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    if(p.getFieldInfoCount != 0) {
      df.show
      val result = anonymize(df)
      result.show
      val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
      Verification(privacy).printAnonymizeResult(df, result, fieldInfos)    
      (result)
    } else {
      val columnIDs: Array[Int] =
        (p.getSelectedColumnIdList map (_.asInstanceOf[Int])).toArray
      anonymize(df, getColumnNames(df, columnIDs))
    }
  }

}

object IdenReductionOperator {
  def apply(o: StreamOperatorInfo): IdenReductionOperator = new IdenReductionOperator(o)
}
