package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.EqualWidthBinningInfo
import ksb.csle.common.proto.StreamOperatorProto.EqualWidthBinningInfo.OutputType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.UnitSpec
import ksb.csle.component.operator.DataFunSuite
import org.scalatest._
import scala.reflect.runtime.universe

class EqualWithBinningTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Array[Integer] = _
  @transient var correctAnswer: Array[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Array(2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14)
    correctAnswer = Array(
        "range1[-Infinity-5.000000]", "range1[-Infinity-5.000000]",
        "range1[-Infinity-5.000000]", "range2[5.000000-8.000000]",
        "range2[5.000000-8.000000]", "range2[5.000000-8.000000]",
        "range3[8.000000-11.000000]", "range3[8.000000-11.000000]",
        "range3[8.000000-11.000000]", "range4[11.000000-Infinity]",
        "range4[11.000000-Infinity]")
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

  "A EqualWidthBinning" should "do equal width binning condition" in {
    val equalWidthBinningInfo = EqualWidthBinningInfo.newBuilder()
      .addSelectedColumnId(0)
      .setNumberOfBins(4)
      .setOutputType(OutputType.NEW_COLUMN)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.leaning.EqualWidthBinning")
      .setEqualWidthBinning(equalWidthBinningInfo)
      .build
    var srcDf = convertArrayToDataframeWithIndex(inputData, "original")
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")
    val equalWidthBinning = EqualWidthBinningOperator(operator1)
    val desDF = equalWidthBinning.operate(srcDf)
    var resultDF = desDF.join(
        correctAnswerDf, desDF.col("index") === correctAnswerDf.col("index"))
    resultDF.select("original_result", "correct_answer").collect().foreach{
      case Row(x: String, y: String) =>
        assert(x === y, "The feature value is not correct after EqualWidthBinning.")
    }
  }
}
