package ksb.csle.didentification.pipe.preprocessing

import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.window

import ksb.csle.common.proto.StreamPipeControlProto.{StreamPipeOperatorInfo, AggregateTimePipeInfo}
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

class TimeAggregator(
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  private val p: AggregateTimePipeInfo = o.getAggregateTime
  private val aggrName = p.getAggrType match {
      case AggregateTimePipeInfo.AggregateType.AGGR_AVERAGE => "avg"
      case AggregateTimePipeInfo.AggregateType.AGGR_COUNT => "count"
      case AggregateTimePipeInfo.AggregateType.AGGR_SUM => "sum"
      case AggregateTimePipeInfo.AggregateType.AGGR_MEDIAN => "median"
      case AggregateTimePipeInfo.AggregateType.AGGR_MIN => "min"
      case AggregateTimePipeInfo.AggregateType.AGGR_MAX => "max"
      case _ => "avg"
    }

  def timeAggregate(df: DataFrame => DataFrame): DataFrame => DataFrame =
    df => {
      logger.info(s"OpId ${o.getId} : Time Aggregator")
      val aggrColumns = df.columns.filterNot(_.equals(p.getTimeColumn))
      val aggrParams = aggrColumns.map(x => x -> aggrName).toMap
      
      // window function causes the confusion of the order of columns
      val result = df.groupBy(
          window(df.col(p.getTimeColumn), 
              p.getWindow.getWindowLength,
              p.getWindow.getSlidingInterval)
          ).agg(aggrParams)
          
      val aggrColumNames = aggrColumns.map(x => "%s(%s)".format(aggrName, x))
      result.sort("window.start")
            .select("window.start", aggrColumNames: _*)
            .toDF(df.columns:_*) // rename the column name of result
    }
  
  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = timeAggregate(df)
}
