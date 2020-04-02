package ksb.csle.component.operator.transformation

import java.sql.Timestamp
import java.lang.String
import scala.collection.JavaConversions._

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.ingestion.util._
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that makes the time windowing rows by time window value parameter.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.MakeTimeWindowRowsInfo]]
 *          MakeTimeWindowRowsInfo contains attributes as follows:
 *          - timeWindowValue: Length of rows to be time-windowed (required)
 *
 *  ==MakeTimeWindowRowsInfo==
 *  {{{
 *  message MakeTimeWindowRowsInfo {
 *  required int32 timeWindowValue = 3;
 *  }
 *  }}}
 */
class ReshapeWithTimeWindowOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.MakeTimeWindowRowsInfo =
    o.getMakeTimeWindowRows

  private def makeTimeWindowRows(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : MakeTimeWindowRows")
    logger.info(df.show(100,false).toString)
    val dfLen = df.count
    val widowSize = p.getTimeWindowValue
    val sp = df.sparkSession
    import sp.sqlContext.implicits._
    var tempDf = df.coalesce(1).toDF
    var resultDf = sp.createDataFrame(sp.sparkContext.emptyRDD[Row], df.schema)
    var count = 0
    while(dfLen-count-widowSize +1 > 0){
      var limitDf = tempDf.limit(widowSize)
      resultDf = resultDf.union(limitDf)
      var tempRdd = tempDf.rdd.mapPartitions(iterator => iterator.drop(1))
      tempDf = sp.sqlContext.createDataFrame(tempRdd, df.schema)
      count = count + 1
    }
    logger.info(resultDf.show(100,false).toString)
    logger.info(resultDf.printSchema.toString)
    resultDf
  }

  /**
   * Operates MakeTimeWindowRows.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = makeTimeWindowRows( df)
}

object ReshapeWithTimeWindowOperator {
  def apply(o: StreamOperatorInfo): ReshapeWithTimeWindowOperator =
    new ReshapeWithTimeWindowOperator(o)
}