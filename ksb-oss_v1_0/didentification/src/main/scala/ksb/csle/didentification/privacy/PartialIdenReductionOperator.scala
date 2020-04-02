package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.exceptions.AnonymityExceptionHandler
import ksb.csle.didentification.verification.Verification
import ksb.csle.didentification.utilities.GeneralizeManager

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the identity partial reduction module in the
 * Data Reduction algorithm, which replace the some parts of the value with "*".
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.IdenPartialReductionInfo]]
 *          IdenPartialReductionInfo contains attributes as follows:
 *          - method: not used currently.
 *          - generalizedColumnInfo: the configuration about the generalization step
 *          - columnHandlePolicy: consider the given columns all together, or individually.
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==IdenPartialReductionInfo==
 *  {{{
 *  enum ReductionMethod {
 *    DELETE = 0;
 *    REPLACE = 1;
 *  }
 *  message GeneralizeColumnInfo {
 *  	required int32 selectedColumnId = 1;
 *  	required int32 numLevels = 2 [default = 5];
 *  	required int32 curLevel = 3 [default = 1];
 *  }
 *  enum ColumnHandlePolicy {
 *    ONEBYONE = 0;
 *    ALL = 1;
 *  }
 *  message IdenPartialReductionInfo {
 *    required ReductionMethod method = 1 [default = DELETE];
 *    repeated GeneralizeColumnInfo generalizedColumnInfo = 2;
 *    required ColumnHandlePolicy columnHandlePolicy = 3 [default = ONEBYONE];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class PartialIdenReductionOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getIdenPartial.getCheck) {

  val p: IdenPartialReductionInfo = o.getIdenPartial

  /**
   * Replace the some parts of the value with "*". The parts are decided by
   * the generalization step. If the given generalized step and its maximum
   * step are 3 and 10, respectively, the 30% of value are replaced.
   *
   * @param src Dataframe to anonymize
   * @return DataFrame Generalized dataframe
   */
  def anonymize(src: DataFrame): DataFrame = {
    var result = src
    p.getGeneralizedColumnInfoList.toList.map(colDesc => {
      result =  AnonymityExceptionHandler(result) { result =>
            GeneralizeManager.generalizing(result, colDesc)
      }
    })

    (result)
  }

  private def getGeneralizeColumnInfo(colId: Int): GeneralizeColumnInfo = {
    val colInfo = p.getGeneralizedColumnInfoList.filter(_.getSelectedColumnId == colId)
    colInfo.apply(0) // extract first item in the colInfo buffer
  }

  /**
   * Dummy function. To be modified.
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    src
  }

  /**
   * Operates iden-partial reduction module for basic de-identification
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

object PartialIdenReductionOperator {
  def apply(o: StreamOperatorInfo): PartialIdenReductionOperator = new PartialIdenReductionOperator(o)
}
