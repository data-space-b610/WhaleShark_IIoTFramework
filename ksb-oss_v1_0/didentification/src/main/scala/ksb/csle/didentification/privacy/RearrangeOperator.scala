package ksb.csle.didentification.privacy

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.exceptions._
import ksb.csle.didentification.utilities.SwappingManager
import ksb.csle.didentification.verification.Verification

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the rearrangement module in the aggregation
 * algorithm. The model swaps some records in the given column each other.
 * - The user can configure the records to be rearranged manually.
 * - Or, the records to be rearranged can be automatically done according
 * to the given ratio. That is, if the ratio is 0.5, the half of total records
 * are randomly rearranged each other.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamDidentProto.RearrangementInfo]]
 *          RearrangementInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the re-arrange function
 *          - columnHandlePolicy: consider the given columns all together, or individually.
 *          - method: how to re-arrange the records (manually or randomly)
 *          - ratio: in case of random method, how amount of data should be re-arranged.
 *          - swapList: re-arrange the data according to the the given list  
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==RearrangementInfo==
 *  {{{
 *  enum RearrangeMethod {
 *  	REARR_RANDOM = 0;
 *  	REARR_MANUAL = 1;
 *  }
 *  message SwapList {
 *  	required int32 fromRowId = 1;
 *  	required int32 toRowId = 2;
 *  }
 *  enum ColumnHandlePolicy {
 *    ONEBYONE = 0;
 *    ALL = 1;
 *  }
 *  message RearrangementInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required ColumnHandlePolicy columnHandlePolicy = 2 [default = ONEBYONE];
 *    required RearrangeMethod method = 3 [default = REARR_RANDOM];
 *    optional double ratio = 4 [default = 0.001];
 *    repeated SwapList swapList = 5;
 *    repeated FieldInfo fieldInfo = 6;
 *    optional PrivacyCheckInfo check = 7;
 *  }
 *  }}}
 */
class RearrangeOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getRearrange.getCheck) {

  val p: RearrangementInfo = o.getRearrange

  /**
   * Rearranges some records of given columns each other.
   *
   * @param src Dataframe to rearrange
   * @param columnNames the array of columns to be rearranged
   * @return DataFrame Rearranged dataframe
   */
  @throws(classOf[DDIFileIOException])
  override def anonymize(
      src: DataFrame,
      columnNames: Array[String]): DataFrame = {
    p.getColumnHandlePolicy match {
      case ColumnHandlePolicy.ONEBYONE =>
        anonymizedOneByOne(src, columnNames)
      case ColumnHandlePolicy.ALL =>
        anonymizedAll(src, columnNames)
    }
  }

  /**
   * Suppose there are some columns in a dataset, and the data handler
   * tries to to apply the rearrangement function on some of that columns.
   * This function applies the function on each specified column individually
   * one-by-one.
   *
   * @param src Dataframe to anonymize
   * @param columnNames the array of columns to apply the rearrangment module
   * @return DataFrame the rearranged dataframe
   */
  def anonymizedOneByOne(src: DataFrame, columnNames: Array[String]): DataFrame = {
    var result = src
    columnNames.map(col => result = anonymizeColumn(result, col))

    (result)
  }

  /**
   * Applies the rearrangement function on given specified column
   *
   * @param src Dataframe to anonymize
   * @param columnName the column to apply the rearrangment module
   * @return DataFrame the rearranged dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame = {
    AnonymityExceptionHandler(src) { src =>
      p.getMethod match {
        case RearrangeMethod.REARR_RANDOM => SwappingManager.
          randomSwappingDataFrame(src, Array(columnName), p.getRatio)
        case RearrangeMethod.REARR_MANUAL => SwappingManager.
          swappingDataFrame(src, Array(columnName), getListFromProtobuf(
              (p.getSwapListList map (_.asInstanceOf[SwapList])).toList))
      }
    }
  }

  /**
   * Applies the rearrangement function on given all specified columns
   * simultaneously.
   *
   * @param src Dataframe to anonymize
   * @param columnName the column to apply the rearrangment module
   * @return DataFrame the rearranged dataframe
   */
  def anonymizedAll(src: DataFrame, columnNames: Array[String]): DataFrame =
    AnonymityExceptionHandler(src){ src =>
        p.getMethod match {
          case RearrangeMethod.REARR_RANDOM => SwappingManager.
            randomSwappingDataFrame(src, columnNames, p.getRatio)
          case RearrangeMethod.REARR_MANUAL => SwappingManager.
            swappingDataFrame(src, columnNames, getListFromProtobuf(
                (p.getSwapListList map (_.asInstanceOf[SwapList])).toList))
        }
      }

  private def getListFromProtobuf(swapList: List[SwapList]): List[(Int, Int)] = {
    var result = List[(Int, Int)]()
    swapList.map(entry => {
      result = result :+ (entry.getFromRowId, entry.getToRowId)
      result = result :+ (entry.getToRowId, entry.getFromRowId)
    })
    (result)
  }

  /**
   * Operates rearrangement module for basic de-identification
   *
   * @param runner BaseRunner to run
   * @param df Input dataframe
   * @return DataFrame Anonymized dataframe
   */
  override def operate(df: DataFrame): DataFrame = {
    val columnIDs: Array[Int] =
      (p.getSelectedColumnIdList map (_.asInstanceOf[Int])).toArray

    df.show
    val result = anonymize(df, getColumnNames(df, columnIDs))
    
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos)    
    result.show

    (result)
  }

}

object RearrangeOperator {
  def apply(o: StreamOperatorInfo): RearrangeOperator = new RearrangeOperator(o)
}
