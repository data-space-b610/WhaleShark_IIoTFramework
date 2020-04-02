package ksb.csle.component.pipe.stream.operator

import scala.collection.JavaConversions._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamPipeControlProto.StreamPipeOperatorInfo
import ksb.csle.common.proto.StreamPipeControlProto.GroupbyPipeInfo
import ksb.csle.common.base.pipe.operator.BaseJoinPipeOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that joins selected join columns by key with multiple dataframes
 * and groups joined records by group-by key.
 *
 * @param o Object that contains  message
 *          [[JoinPipeInfo]]
 *          JoinPipeInfo contains attributes as follows:
 *          - key: join key (required)
 *          - joinColumns: columns to join (required)
 *          - joinType: Type of join (optional)
 *          - GroupbyPipeInfo contains attributes as follows:
 *            - timeColName: Column name containing time information (optional)
 *            - keyColName: A list of column names to be used as group by keys
 *                          (required)
 *            - valColName: A list of column name to be used as a value columns
 *                          (required)
 *            - groupby:    Method for aggregation. (required)
 *            - window:     Window information including key, time window length,
 *                        and sliding interval.
 *
 *
 *  ==JoinPipeInfo==
 *  {{{
 *  message JoinPipeInfo {
 *  required string key = 1;
 *  repeated string joinColumns = 2;
 *  optional JoinType joinType = 3 [default = INNER];
 *  optional GroupbyPipeInfo groupBy = 7;
 *  }
 *  }}}
 *
 *  ==JoinType==
 *  {{{
 *  enum JoinType {
 *  INNER = 0;
 *  LEFT_OUTER = 1;
 *  RIGHT_OUTER = 2;
 *  }
 *  }}}
 *
 *  ==GroupbyPipeInfo==
 *  {{{
 *  message GroupbyPipeInfo {
 *   optional string timeColName = 3;
 *   repeated string keyColName = 4;
 *   repeated string valColName = 5;
 *   enum GroupbyOp {
 *     SUM = 0;
 *     COUNT = 1;
 *     AVG = 2;
 *     MIN = 3;
 *     MAX = 4;
 *   }
 *   required GroupbyOp groupby = 6;
 *   optional Window window = 7;
 *  }
 *  }}}
 *
 *  ==Window==
 *  {{{
 *  message Window {
 *  required string key = 1;
 *  required string windowLength = 2;
 *  required string slidingInterval = 3;
 *  }
 *  }}}
 */
class StreamJoinOperator[T](
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseJoinPipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  val p = o.getJoin
  private val joinColumns = p.getJoinColumnsList.map(_.asInstanceOf[String]).toArray

  private def operate(df: Seq[DataFrame]): DataFrame =
    df match {
          case t1 :: Nil => t1.select(p.getKey, joinColumns:_*)
          case t1 :: t2 :: Nil =>
            t1.select(p.getKey, joinColumns:_*)
              .join(t2.select(p.getKey, joinColumns:_*), p.getKey)
          case h :: tail =>
            h.select(p.getKey, joinColumns:_*).join(operate(tail), p.getKey)
        }

  /**
   * Joins multiple dataframe by key.
   *
   * @param  df Input function pipe
   * @return output function pipe
   */
  override def operate(
      df: Seq[Any => DataFrame]): Seq[DataFrame] => DataFrame = df => operate(df)
}
