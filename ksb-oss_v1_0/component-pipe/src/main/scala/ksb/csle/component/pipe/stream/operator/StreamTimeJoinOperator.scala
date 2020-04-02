package ksb.csle.component.pipe.stream.operator

import scala.collection.JavaConversions._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import ksb.csle.common.proto.StreamPipeControlProto.StreamPipeOperatorInfo
import ksb.csle.common.proto.StreamPipeControlProto.JoinWithTimeInfo
import ksb.csle.common.proto.StreamPipeControlProto.JoinType
import ksb.csle.common.base.pipe.operator.BaseJoinPipeOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that joins columns in multiple dataframes by key within time slot.
 *
 * @param o Object that contains  message
 *          [[JoinWithTimeInfo]]
 *          JoinWithTimeInfo contains attributes as follows:
 *          - timeColName: Column name containing time information (required)
 *          - keyColName: A list of column names to be used as group by keys
 *                          (required)
 *          - timeInterval: size of time slot in seconds or minutes. (required)
 *          - joinType: Type of join (optional)
 *
 *  ==JoinWithTimeInfo==
 *  {{{
 *  message JoinWithTimeInfo {
 *   optional string timeColName = 3;
 *   repeated string keyColName = 4;
 *  required string timeInterval = 3 [default = "1 minutes"];
 *  optional JoinType joinType = 4 [default = INNER];
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
 */
class StreamTimeJoinOperator[T](
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseJoinPipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  val p: JoinWithTimeInfo = o.getTimeJoin
  val df_keyCol = p.getKeyColName
  val df2_keyCol = p.getKeyColName + "_"
  val df_timeCol = p.getTimeColName
  val df2_timeCol = p.getTimeColName + "_"
  val command: String = (
      df_keyCol + " = " + df2_keyCol + " AND "
      + df_timeCol + " >= " + df2_timeCol + " AND "
      + df_timeCol + " <= " + df2_timeCol + " + interval "
      + p.getTimeInterval)
  val joinOp = p.getJoinType match {
    case JoinType.LEFT_OUTER => "leftOuter"
    case JoinType.RIGHT_OUTER => "rightOuter"
    case _ => "inner"
  }

  private def timeJoin(df1: DataFrame, df2: DataFrame): DataFrame = {
    val tmpDF2 = df2.withColumnRenamed(df_keyCol, df2_keyCol)
      .withColumnRenamed(df_timeCol, df2_timeCol)

    df1.join(tmpDF2, expr(command), joinType = joinOp)
      .drop(df2_keyCol)
      .drop(df2_timeCol)
  }

  private def operate(df: Seq[DataFrame]): DataFrame =
    df match {
          case t1 :: Nil => t1
          case t1 :: t2 :: Nil => timeJoin(t2, t1)
          case h :: tail => timeJoin(operate(tail), h)
        }

  override def operate(
      df: Seq[Any => DataFrame]): Seq[DataFrame] => DataFrame = df => operate(df.reverse)
}
