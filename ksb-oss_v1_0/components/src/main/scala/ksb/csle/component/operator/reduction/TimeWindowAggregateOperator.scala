package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto.AggregateTimeWindowInfo.Scope
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that aggregates time window in the given dataframe.
 * Note: Input dataframe must have a Unix time stamp column(in seconds).
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AggregateTimeWindowInfo]]
 *          AggregateTimeWindowInfo contains attributes as follows:
 *          - scope: Scope to be applied. Enum(SCOPE_ALL, SCOPE_SELECTED) (required)
 *          - selectedColumnId: Column ids to be aggregated by given condition
 *                              (repeated)
 *          - subParam: Parameter to specify whether the option to aggregate
 *                      (required) Ex: {"minute", "10"},{"hour", "1"}
 *
 *  ==AggregateTimeWindowInfo==
 *  {{{
 *  message AggregateTimeWindowInfo {
 *  required Scope scope = 4 [default = SCOPE_ALL];
 *  repeated int32 selectedColumnId = 5;
 *  repeated SubParameter subParam = 6;
 *  enum Scope {
 *    SCOPE_ALL = 0;
 *    SCOPE_SELECTED = 1;
 *  }
 *  }
 *  }}}
 */
class TimeWindowAggregateOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o){

  val p: AggregateTimeWindowInfo = o.getAggregateTimeWindow

  private def aggregateTimeWindow(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : AggregateTimeWindow")
    if(!df.columns.contains("timestamp")) {
      logger.error("Check TimeIndex")
      throw new RuntimeException("timestame does not exist")
    }
    val columnNames: Array[String] =
      if (p.getScope.equals(Scope.SCOPE_ALL)) {
        df.columns
      } else {
        Utils.getColumnNames(p.getSelectedColumnIdList, df)
      }
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    val groupByCols: Array[String] =
      if(options.contains("groupBy")) {
        options.get("groupBy").get.split(",").map(x => df.columns(x.trim.toInt))
      } else {
        Array()
      }
    val windowDuration: String =
      if(options.contains("hour")) {
        options.get("hour").get.trim.toInt.toString + " hour"
      } else if(options.contains("minute")) {
        options.get("minute").get.trim.toInt.toString + " minute"
      } else if(options.contains("second")) {
        options.get("second").get.trim.toInt.toString + " second"
      } else if(options.contains("day")) {
        options.get("day").get.trim.toInt.toString + " day"
      } else {
        throw new RuntimeException(
            """Check Option about windowDuration (e.g., 'minute')""")
      }
    val agg_method: String =
      if(options.contains("method")) {
        options.get("method").get.trim.toLowerCase
      } else {
        "avg"
      }
    val agg_method_checked = Utils.checkAggregateMethods(agg_method)
    val tempMap = scala.collection.mutable.Map[String, String]()
    columnNames.map(x => tempMap += x -> agg_method_checked)
    tempMap.remove("timestamp")
    groupByCols.map(x => tempMap.remove(x))
    import org.apache.spark.sql.functions._
    val windowCol: Column = window(df("timestamp"), windowDuration)
    val groupCols: Array[Column] = windowCol +: groupByCols.map(x => df.col(x))
    val temp_result = df
      .groupBy(groupCols:_*).agg(tempMap.toMap)
      .orderBy("window", groupByCols:_*)
    val tempColNames: Array[String] =
      temp_result.columns.map(x => x.substring(x.indexOf("(")+1, x.length-1))
    var result = temp_result
    temp_result.columns.slice(1, temp_result.columns.size).map(
      x => result = result.withColumn(
          x.substring(x.indexOf("(")+1, x.length-1), result.col(x)))
    result = result.drop(
      temp_result.columns.slice(1, temp_result.columns.size):_*)
    logger.info(result.show.toString)
    logger.info(result.printSchema.toString)
    result
  }

  /**
   * Operates aggregateTimeWindow.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame
      = aggregateTimeWindow(df)
}

object TimeWindowAggregateOperator {
  def apply(o: StreamOperatorInfo) = new TimeWindowAggregateOperator(o)
}