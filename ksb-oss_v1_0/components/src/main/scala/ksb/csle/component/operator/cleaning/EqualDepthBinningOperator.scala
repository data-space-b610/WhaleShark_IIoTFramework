package ksb.csle.component.operator.cleaning

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.collection.JavaConversions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.EqualDepthBinningInfo
import ksb.csle.common.proto.StreamOperatorProto.EqualDepthBinningInfo.OutputType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that makes equal-depth-binning.
 * It divides the range into N intervals, each containing approximately same
 * number of samples.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.EqualDepthBinningInfo]]
 *          EqualDepthBinningInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be divided by equal-depth-binnig (repeated)
 *          - numberOfBins: Number of bins (required)
 *          - outputType: Type of output. Enum(NEW_COLUMN, VALUE_CHANGE), (required)
 *
 *  ==EqualDepthBinningInfo==
 *  {{{
 *  message EqualDepthBinningInfo {
 *  repeated int32 selectedColumnId = 3;
 *  required int32 numberOfBins = 4;
 *  required OutputType outputType = 5 [default = NEW_COLUMN];
 *  enum OutputType {
 *       NEW_COLUMN = 0;
 *       VALUE_CHANGE = 1;
 *  }
 *  }
 *  }}}
 */
class EqualDepthBinningOperator(
    o: StreamOperatorInfo) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.EqualDepthBinningInfo =
    o.getEqualDepthBinning

  /**
   * Validates equalDepthBinning info and dataframe schema info using following params.
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
        val uniqSize = df.select(df(selectedColumnName)).na.fill(0).distinct.count()
        if (uniqSize < p.getNumberOfBins)
          throw new DataException(s"Too many Number Of Bins Data : $p.getNumberOfBins")
      }
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  /**
   * Runs equalDepthBinning operation using following params.
   */
  @throws(classOf[KsbException])
  private def equalDepthBinning(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : EqualDepthBinning")
    val numberOfBins = p.getNumberOfBins
    val outputType = p.getOutputType
    var result = validate(df)
    try {
      for (selectedColumnId <- p.getSelectedColumnIdList.toArray) {
        val selectedColumnName: String =
          Utils.getSingleColumnName(selectedColumnId.toString().toInt, df)
        var intervals =
          makeIntervals(
              df.selectExpr(
                  s"cast($selectedColumnName as double) $selectedColumnName")
          .sort(asc(selectedColumnName))
          .collect.map(r => r.getDouble(0)), numberOfBins)
        val makeResult =
          udf[String, String](x => changeValue(x.toDouble, intervals))
        if (outputType == OutputType.NEW_COLUMN) {
          result = result.withColumn(
              selectedColumnName + "_result", makeResult(df(selectedColumnName)))
        } else if (outputType == OutputType.VALUE_CHANGE) {
          result = result.withColumn(
              selectedColumnName, makeResult(df(selectedColumnName)))
        }
      }
    } catch {
      case e: Exception => throw new ProcessException(s"EqualDepthBinning Process Error : ", e)
    }
    result
  }

  private def makeBinDataSize(
      numberOfBins: Int,
      rest: Int,
      defaultSize: Int): Array[Int] = {
    val binsDataSize = new Array[Int](numberOfBins)
    var nextSize = defaultSize
    for (i <- 0 until numberOfBins by 1) {
      breakable {
        if (rest == 0) {
          binsDataSize(i) = defaultSize
          break
        }
        if (numberOfBins >= rest * 2) {
          if (numberOfBins - i <= rest * 2) {
            binsDataSize(i) = nextSize
            if (nextSize == defaultSize)
              nextSize = defaultSize + 1
            else
              nextSize = defaultSize
            break
          } else {
            binsDataSize(i) = defaultSize
          }
        } else {
          if (rest + i <= numberOfBins) {
            binsDataSize(i) = nextSize
            if (nextSize == defaultSize)
              nextSize = defaultSize + 1
            else
              nextSize = defaultSize
          } else {
            binsDataSize(i) = defaultSize + 1
          }
        }
      }
    }
    binsDataSize
  }

  /**
   * Runs makeIntervals operation using following params.
   */
  @throws(classOf[KsbException])
  private def makeIntervals(
      data: Array[Double],
      numberOfBins: Int): Array[Double] = {
    val size: Double = data.length / numberOfBins.toDouble
    var rest = data.length % numberOfBins
    val binsDataSize = makeBinDataSize(numberOfBins, rest, size.toInt)
    val intervals = new Array[Double](numberOfBins * 2)
    val restEmptyCount = 0
    intervals(0) = Double.NegativeInfinity
    var binsIndex = 0
    var goalCount = binsDataSize(binsIndex)
    var count = 0
    var beforeData = 0d
    var maxCount = goalCount + binsDataSize(binsIndex + 1)
    for (i <- 0 until data.length by 1) {
      val x = data(i)
//      if (i == data.length - 1) {
//        intervals(intervals.length - 1) = Double.PositiveInfinity
//        return intervals
//      }
      if (count > maxCount) {
        throw new KsbException("Too many equal values")
      }
      if (count >= goalCount && beforeData != x) {
        intervals(binsIndex * 2 + 1) = (x - beforeData) / 2 + beforeData
        intervals(binsIndex * 2 + 2) = intervals(binsIndex * 2 + 1)
        binsIndex = binsIndex + 1
        goalCount = binsDataSize(binsIndex) + goalCount - count
        if (binsIndex == binsDataSize.length - 1)
          maxCount = goalCount
        else
          maxCount = goalCount + binsDataSize(binsIndex + 1)
        count = 0
      }
      count = count + 1
      beforeData = x
    }
    intervals(intervals.length - 1) = Double.PositiveInfinity
    intervals
  }

  /**
   * Runs changeValue operation using following params.
   */
  private def changeValue(data: Double, intervals: Array[Double]): String = {
    var result = ""
    breakable {
      for (i <- 0 until intervals.length by 2) {
        if (data >= intervals(i) && data < intervals(i + 1)) {
          result = "range%d[%2f-%2f]".format(i / 2 + 1, intervals(i), intervals(i + 1))
          break
        }
      }
    }
    result
  }

  /**
   * Operates EqualDepthBinning.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = equalDepthBinning(df)
}

object EqualDepthBinningOperator {
  def apply(o: StreamOperatorInfo): EqualDepthBinningOperator =
    new EqualDepthBinningOperator(o)
}