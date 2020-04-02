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
 * Operator that full joins multiple dataframes by key.
 *
 * @param o Object that contains  message
 *          [[AllJoinPipeInfo]]
 *          AllJoinPipeInfo contains attributes as follows:
 *          - key: key to join
 *
 *  ==SelectColumnsInfo==
 *  {{{
 *  message AllJoinPipeInfo {
 *    required string key = 1;
 *  }
 *  }}}
 */
class StreamAllJoinOperator[T](
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseJoinPipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  val p = o.getAllJoin

  private def operate(df: Seq[DataFrame]): DataFrame =
    df match {
          case t1 :: Nil => t1
          case t1 :: t2 :: Nil =>
            t1.join(t2, p.getKey)
          case h :: tail =>
            h.join(operate(tail), p.getKey)
        }

  /**
   * Joins all columns in multiple dataframe by key.
   *
   * @param  df Input function pipe
   * @return output function pipe
   */
  override def operate(
      df: Seq[Any => DataFrame]): Seq[DataFrame] => DataFrame = df => operate(df)
}
