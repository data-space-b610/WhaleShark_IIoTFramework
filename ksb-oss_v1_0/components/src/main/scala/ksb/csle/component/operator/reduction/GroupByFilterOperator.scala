package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.GroupbyFilterInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto.GroupbyFilterInfo.Condition
import ksb.csle.common.proto.StreamOperatorProto.GroupbyFilterInfo.GroupbyOp
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that selects which rows in a given column of input dataframe should
 * be kept and which rows should be removed. Rows satisfying the given condition
 * are kept, remaining rows are removed.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.GroupbyFilterInfo]]
 *          GroupbyFilterInfo contains attributes as follows:
 *          - keyColName: Column name to be filtered in the given input dataframe
 *                        (required)
 *          - valColName: Column name to be used as a value column (optional)
 *          - groupby:    method for aggregation. Enum(SUM, COUNT), (required)
 *          - condition: Condition to select rows. Enum(EQUAL,LESS_THAN,
 *                       LARGE_THAN,LESS_EQUAL,LARGE_EQUAL) (required)
 *          - value: Base value to compare with the value of row (required)
 *
 *  ==GroupbyFilterInfo==
 *  {{{
 *  message GroupbyFilterInfo {
 *  required string keyColName = 1;
 *  optional string valColName = 2;   
 *  required GroupbyOp groupby = 3;
 *  required Condition condition = 4 [default = EQUAL];
 *  required int32 value = 5;
 *  enum GroupbyOp {
 *    SUM = 0;
 *    COUNT = 1;
 *  }
 *  enum Condition {
 *    EQUAL = 0;
 *    LESS_THAN = 1;
 *    LARGE_THAN = 2;
 *    LESS_EQUAL = 3;
 *    LARGE_EQUAL = 4;
 *  }
 *  }
 *  }}}
 */
class GroupByFilterOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: GroupbyFilterInfo = o.getGroupbyFilter
  
  private def groupby(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : GroupByFilter")
    
    val result = p.getGroupby match {
      case GroupbyOp.COUNT => filter_count(df)
      //TO DO: implement filter_sum()
      case GroupbyOp.SUM => df
      case _ => df
    }
    logger.debug(result.show.toString)
    logger.info(s"Row Count : ${result.count().toString()}")

    result
  }
  private def filter_sum(df: DataFrame): DataFrame =
          p.getCondition match {
            case Condition.EQUAL =>
              df.groupBy(p.getKeyColName).sum(p.getValColName)
                .filter(s"${p.getValColName} == ${p.getValue}")
            case Condition.LESS_THAN =>
              df.groupBy(p.getKeyColName).sum(p.getValColName)
                .filter(s"${p.getValColName} < ${p.getValue}")
            case Condition.LESS_EQUAL =>
              df.groupBy(p.getKeyColName).sum(p.getValColName)
                .filter(s"${p.getValColName} <= ${p.getValue}")
            case Condition.LARGE_THAN =>
              df.groupBy(p.getKeyColName).sum(p.getValColName)
                .filter(s"${p.getValColName} > ${p.getValue}")
            case Condition.LARGE_EQUAL =>
              df.groupBy(p.getKeyColName).sum(p.getValColName)
                .filter(s"${p.getValColName} >= ${p.getValue}")
            case _ => df
            }
  
  private def filter_count(df: DataFrame): DataFrame = {
    val colNames = df.columns
    val countDF = p.getCondition match {
            case Condition.EQUAL =>
              df.groupBy(p.getKeyColName).count()
                .filter(s"${"count"} == ${p.getValue}")
            case Condition.LESS_THAN =>
              df.groupBy(p.getKeyColName).count()
                .filter(s"${"count"} < ${p.getValue}")
            case Condition.LESS_EQUAL =>
              df.groupBy(p.getKeyColName).count()
                .filter(s"${"count"} <= ${p.getValue}")
            case Condition.LARGE_THAN =>
              df.groupBy(p.getKeyColName).count()
                .filter(s"${"count"} > ${p.getValue}")
            case Condition.LARGE_EQUAL =>
              df.groupBy(p.getKeyColName).count()
                .filter(s"${"count"} >= ${p.getValue}")
            case _ => df
            }
    logger.debug(df.groupBy(p.getKeyColName).count().show.toString)
    val newDF = df.join(countDF, Seq(p.getKeyColName))
                  .drop("count")
    newDF.select(colNames.head, colNames.tail:_*)
  }
  /**
   * Operates filtering function for data cleaning
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = groupby(df)
}

object GroupByFilterOperator {
  def apply(o: StreamOperatorInfo): GroupByFilterOperator = new GroupByFilterOperator(o)
}