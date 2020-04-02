package ksb.csle.component.operator.cleaning

import scala.util.control.Breaks._
import scala.collection.JavaConversions._

import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.EqualWidthBinningInfo
import ksb.csle.common.proto.StreamOperatorProto.EqualWidthBinningInfo.OutputType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that makes equal-width-binning.
 * It divides the range into N intervals of equal size(uniform grid).
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.EqualWidthBinningInfo]]
 *          EqualWidthBinningInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be divided by equal-width-binnig. (repeated)
 *          - numberOfBins: Number of bins (required)
 *          - outputType: Type of output. Enum(NEW_COLUMN, VALUE_CHANGE), (optional)
 *
 *  ==EqualWidthBinningInfo==
 *  {{{
 *  message EqualWidthBinningInfo {
 *  repeated int32 selectedColumnId = 3;
 *  required int32 numberOfBins = 4;
 *  optional OutputType outputType = 5 [default = NEW_COLUMN];
 *  enum OutputType {
 *       NEW_COLUMN = 0;
 *       VALUE_CHANGE = 1;
 *  }
 *  }
 *  }}}
 */

class EqualWidthBinningOperator(
    o: StreamOperatorInfo) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.EqualWidthBinningInfo =
    o.getEqualWidthBinning

  /**
   * Validates equalWidthBinning info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
      if (p.getNumberOfBins <= 1)
        throw new DataException(s"Too small Number Of Bins Data : $p.getNumberOfBins")
      for (selectedColumnId <- p.getSelectedColumnIdList.toArray) {
        val selectedColumnName: String =
          Utils.getSingleColumnName(selectedColumnId.toString().toInt, df)
        val maxMinDf =
          df.selectExpr(
              s"cast($selectedColumnName as double) $selectedColumnName")
            .agg(max(selectedColumnName), min(selectedColumnName))
        val range =
          maxMinDf.collect()(0).getDouble(0) - maxMinDf.collect()(0).getDouble(1)
        if (range <= 0) {
          throw new DataException(s"Max - Min Zero , column $selectedColumnName")
        }
      }
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  @throws(classOf[KsbException])
  private def equalWidthBinning(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : EqualWidthBinning")
    val numberOfBins = p.getNumberOfBins
    val outputType = p.getOutputType
    var result = validate(df)
    try {
      for(selectedColumnId <- p.getSelectedColumnIdList.toArray) {
        val selectedColumnName: String =
          Utils.getSingleColumnName(selectedColumnId.toString().toInt, df)
        logger.info("selectColumn : " + selectedColumnName)
        var intervals =
          makeIntervals(
              df.selectExpr(
                  s"cast($selectedColumnName as double) $selectedColumnName")
                  .collect
                  .map(r => r.getDouble(0)), numberOfBins)
        logger.info(intervals.toList)
        val makeResult =
          udf[String, String] (x => changeValue(x.toDouble, intervals))
        if(outputType == OutputType.NEW_COLUMN) {
          result =
            result.withColumn(
                selectedColumnName + "_result", makeResult(df(selectedColumnName)))
        } else if(outputType == OutputType.VALUE_CHANGE) {
          result =
            result.withColumn(selectedColumnName, makeResult(df(selectedColumnName)))
        }
      }
    } catch {
      case e: Exception => throw new ProcessException(s"EqualWidthBinning Process Error : ", e)
    }
    result
  }

  /**
   * Runs makeIntervals operation using following params.
   */
  private def makeIntervals(
      data: Array[Double],
      numberOfBins: Int): Array[Double] = {
    val max = data.max
    val min = data.min
    val width = (max - min) / numberOfBins
    val intervals = new Array[Double](numberOfBins*2)
    intervals(0) = min
    intervals(1) = min + width
    for(i <- 2 until intervals.length by 2) {
      intervals(i) = intervals(i - 1);
      intervals(i + 1) = intervals(i) + width;
    }
    intervals(0) = Double.NegativeInfinity;
    intervals(intervals.length-1) = Double.PositiveInfinity;
    intervals
  }

  /**
   * Runs changeValue operation using following params.
   */
  private def changeValue(
      data: Double,
      intervals: Array[Double]): String = {
    var result = ""
    breakable{
      for(i <- 0 until intervals.length by 2) {
        if (data >= intervals(i) && data < intervals(i + 1)) {
          result =  "range%d[%2f-%2f]".format(i/2 + 1, intervals(i), intervals(i + 1))
          break
        }
      }
    }
    result
  }

  /**
   * Operates equalWidthBinning.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = equalWidthBinning(df)
}

object EqualWidthBinningOperator {
  def apply(o: StreamOperatorInfo): EqualWidthBinningOperator =
    new EqualWidthBinningOperator(o)
}