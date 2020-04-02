package ksb.csle.component.pipe.stream.operator

import org.apache.spark.sql.{ SparkSession, DataFrame }
import ksb.csle.common.proto.StreamPipeControlProto.StreamPipeOperatorInfo
import ksb.csle.common.proto.StreamPipeControlProto.FilterPipeInfo
import ksb.csle.common.proto.StreamPipeControlProto.FilterPipeInfo.Condition
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that selects which rows in a given column of input dataframe should
 * be kept and which rows should be removed. Rows satisfying the given condition
 * are kept, remaining rows are removed.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.FilterInfo]]
 *          FilterInfo contains attributes as follows:
 *          - columnName: Column name to be filtered in the given input dataframe
 *                        (required)
 *          - condition: Condition to select rows. Enum(EQUAL,LESS_THAN,
 *                       LARGE_THAN,LESS_EQUAL,LARGE_EQUAL,EXIST,LIKE) (required)
 *          - value: Base value to compare with the value of row (optional)
 *          - pattern: String pattern to compare with the string value of row
 *                     (optional)
 *
 *  ==FilterPipeInfo==
 *  {{{
 *  message FilterInfo {
 *  required string colName = 4;
 *  required Condition condition = 5 [default = EQUAL];
 *  optional int32 value = 6;
 *  optional string pattern = 7;
 *  enum Condition {
 *    EQUAL = 0;
 *    LESS_THAN = 1;
 *    LARGE_THAN = 2;
 *    LESS_EQUAL = 3;
 *    LARGE_EQUAL = 4;
 *    EXIST = 5;
 *    LIKE = 6;
 *  }
 *  }
 *  }}}
 */
class FilterOperator(
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  val p: FilterPipeInfo = o.getFilter

  private def filter(df: DataFrame => DataFrame): DataFrame => DataFrame =
    df =>
      p.getCondition match {
        case Condition.EQUAL =>
          df.filter(s"${p.getColName} == ${p.getValue}")
        case Condition.LESS_THAN =>
          df.filter(s"${p.getColName} < ${p.getValue}")
        case Condition.LESS_EQUAL =>
          df.filter(s"${p.getColName} <= ${p.getValue}")
        case Condition.LARGE_THAN =>
          df.filter(s"${p.getColName} > ${p.getValue}")
        case Condition.LARGE_EQUAL =>
          df.filter(s"${p.getColName} >= ${p.getValue}")
        case Condition.EXIST =>
          df.filter(df(p.getColName).contains(p.getPattern))
        case Condition.LIKE =>
          df.filter(df(p.getColName).rlike(p.getPattern))
        case _ => df
      }

  /**
   * Operates filtering function for data cleaning
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = filter(df)
}
