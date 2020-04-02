package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.EqualDepthBinningInfo
import ksb.csle.common.proto.StreamOperatorProto.EqualDepthBinningInfo.OutputType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.UnitSpec
import ksb.csle.component.operator.DataFunSuite
import org.scalatest._
import scala.reflect.runtime.universe

class EqualDepthBinningTest extends UnitSpec with DataFunSuite with Logging  {
  @transient var data: Array[Integer] = _
  @transient var correctAnswer: Array[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = Array(0, 4, 12, 16, 16, 18, 18, 18, 28)
    correctAnswer = Array(
      "range1[-Infinity-14.000000]", "range1[-Infinity-14.000000]",
      "range1[-Infinity-14.000000]", "range2[14.000000-23.000000]",
      "range2[14.000000-23.000000]", "range2[14.000000-23.000000]",
      "range2[14.000000-23.000000]", "range2[14.000000-23.000000]",
      "range3[23.000000-Infinity]")
  }

  def convertArrayToDataframeWithIndex(
    input: Array[Integer],
    name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
      spark.createDataFrame(input.zipWithIndex).toDF(name, "index")
    df
  }

  def convertArrayToDataframeWithIndex(
    input: Array[String],
    name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
      spark.createDataFrame(input.zipWithIndex).toDF(name, "index")
    df
  }

  "A EqualDepthBinning" should "do equal depth binning condition" in {
    val equalDepthBinningInfo = EqualDepthBinningInfo.newBuilder()
      .addSelectedColumnId(0)
      .setNumberOfBins(3)
      .setOutputType(OutputType.NEW_COLUMN)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.leaning.EqualDepthBinning")
      .setEqualDepthBinning(equalDepthBinningInfo)
      .build
    var srcDf = convertArrayToDataframeWithIndex(data, "original")
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")
    val equalDepthBinning = EqualDepthBinningOperator(operator1)
    val desDF = equalDepthBinning.operate(srcDf)
    var resultDF = desDF.join(
        correctAnswerDf, desDF.col("index") === correctAnswerDf.col("index"))
    resultDF.select("original_result", "correct_answer").collect().foreach{
      case Row(x: String, y: String) =>
        assert(x === y, "The feature value is not correct after EqualDepthBinning.")
    }
  }
}
