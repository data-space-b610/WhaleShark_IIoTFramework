package ksb.csle.component.pipe.stream.operator

import scala.collection.JavaConversions._

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamPipeControlProto.StreamPipeOperatorInfo
import ksb.csle.common.proto.StreamPipeControlProto.GroupbyPipeInfo
import ksb.csle.common.proto.StreamPipeControlProto.GroupbyPipeInfo.GroupbyOp
import ksb.csle.common.base.pipe.operator.BasePipeOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that pipelines dataframe by aggregating records
 * according to a given group condition.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.GroupbyPipeInfo]]
 *          GroupbyPipeInfo contains attributes as follows:
 *          - timeColName: Column name containing time information
 *                        (optional)
 *          - keyColName: A list of column names to be used as group by keys
 *                        (required)
 *          - valColName: A list of column name to be used as a value columns
 *                        (required)
 *          - groupby:    Method for aggregation. (required)
 *          - window:     Window information including key, time window length,
 *                        and sliding interval.
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
 * }
 * }}}
 */
class GroupByOperator(
    o: StreamPipeOperatorInfo,
    session: SparkSession
    ) extends BasePipeOperator[DataFrame, StreamPipeOperatorInfo, SparkSession](
        o,
        session) {

  private val p: GroupbyPipeInfo = o.getGroupby
  private val method =  p.getGroupby match {
    case GroupbyOp.MIN => "min"
    case GroupbyOp.MAX => "max"
    case GroupbyOp.AVG => "avg"
    case GroupbyOp.SUM => "sum"
    case GroupbyOp.COUNT => "count"
    case _ => "avg"
  }

  private val timestampCol =
    if (p.hasTimeColName()) p.getTimeColName
    else "timestamp"

  private val keyColnames: Array[String] =
    p.getKeyColNameList.map(col => col).toArray
  private val valColNames = p.getValColNameList.map(col => col).toArray
  private val colNames = keyColnames ++ valColNames
  private val reNames =
    if(p.hasWindow()) keyColnames ++ Array("window") ++ valColNames
    else keyColnames ++ Array(timestampCol) ++ valColNames

  /**
   * Operates groupby function for data integration
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: Any => DataFrame): DataFrame => DataFrame = df => {
    import session.implicits._

    var columns = p.getKeyColNameList.map(col => df(col)).toArray
    val groupdf =
      if (p.hasWindow()) {
        columns :+= window(
          df.col(timestampCol),
          p.getWindow.getWindowLength,
          p.getWindow.getSlidingInterval)
      // TODO: Implement multiple aggregate.
        df.groupBy(columns: _*)
          .agg(p.getValColName(0) -> method)
      } else {
        df.groupBy(columns: _*)
          .agg(p.getValColName(0) -> method)
      }

    groupdf.toDF(reNames: _*)

//    val groupColumnName =
//      df.columns.filterNot(_.equals(p.getTimeColName)).map(x =>
//      if(x == p.getKeyColName(0)) x
//      else "%s(%s)".format(method, x))
//
//    result.select("window.start", colNames:_*)
//          .toDF(df.columns:_*)
  }
}
