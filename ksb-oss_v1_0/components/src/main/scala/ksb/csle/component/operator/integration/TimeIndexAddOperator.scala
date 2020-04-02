package ksb.csle.component.operator.integration

import scala.collection.JavaConversions._

import com.google.protobuf.Message
import com.google.common.collect.ImmutableMap

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that adds a time index column to the input dataframe using a given
 * condition. The time index column generated from the column with time string
 * pattern has Unix time stamp (in seconds).
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AddTimeIndexColumnInfo]]
 *          AddTimeIndexColumnInfo contains attributes as follows:
 *          - userTimeIndexColumnId: Column id to be converted to Unix time
 *                                   stamp (in seconds) (required)
 *          - userTimeIndexPattern: Time string pattern of userTimeIndexColumn
 *                                  (required)
 *
 *  ==AddTimeIndexColumnInfo==
 *  {{{
 *  message AddTimeIndexColumnInfo {
 *  required int32 userTimeIndexColumnId = 4;
 *  required string userTimeIndexPattern = 5;
 *  }
 *  }}}
 */
class TimeIndexAddOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.AddTimeIndexColumnInfo =
    o.getAddTimeIndexColumn

  private def addTimeIndexColumn(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : AddTimeIndexColumn")
    val singleColumnName: String =
      Utils.getSingleColumnName(p.getUserTimeIndexColumnId, df)
    val result =
      df.withColumn(
        "timestamp", unix_timestamp(df(singleColumnName), p.getUserTimeIndexPattern)
        .cast("timestamp"))
    logger.info(result.show.toString)
    logger.info(result.printSchema.toString)
    result
  }

  /**
   * Operates AddTimeIndexColumn.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = addTimeIndexColumn(df)
}

object TimeIndexAddOperator {
  def apply(o: StreamOperatorInfo): TimeIndexAddOperator =
    new TimeIndexAddOperator(o)
}