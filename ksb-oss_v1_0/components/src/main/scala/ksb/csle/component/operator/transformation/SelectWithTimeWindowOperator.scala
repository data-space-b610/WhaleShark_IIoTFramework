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
 * Operator that selects the time windowing rows by time window value parameter.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SelectTimeWindowRowsInfo]]
 *          SelectTimeWindowRowsInfo contains attributes as follows:
 *          - timeWindowValue: Length of rows to be time-windowed (required)
 *
 *  ==SelectTimeWindowRowsInfo==
 *  {{{
 *  message SelectTimeWindowRowsInfo {
 *  required int32 timeWindowValue = 3;
 *  }
 *  }}}
 */
class SelectWithTimeWindowOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.SelectTimeWindowRowsInfo =
    o.getSelectTimeWindowRows

  private def selectTimeWindowRows(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : SelectTimeWindowRows")
    val sp = df.sparkSession
    import sp.sqlContext.implicits._
    val dfLen = df.count
    val widowSize = p.getTimeWindowValue
    var tempDf = df.coalesce(1).toDF
    var resultDf = sp.createDataFrame(sp.sparkContext.emptyRDD[Row], df.schema)
    var count = 0
    while(dfLen-count> 0){
      var limitDf = tempDf.limit(widowSize)
      println("count" + count)
      logger.info(limitDf.show.toString)
      logger.info(limitDf.printSchema.toString)
      var tempRdd = tempDf.rdd.mapPartitions{
        case iterator => iterator.drop(widowSize)
      }
      tempDf = sp.sqlContext.createDataFrame(tempRdd, df.schema)
      count = count + widowSize
    }
    resultDf
  }

  /**
   * Operates SelectTimeWindowRows.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = selectTimeWindowRows(df)
}

object SelectWithTimeWindowOperator {
  def apply(o: StreamOperatorInfo): SelectWithTimeWindowOperator =
    new SelectWithTimeWindowOperator(o)
}