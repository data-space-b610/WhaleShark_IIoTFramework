package ksb.csle.component.operator.reduction

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{ DataFrame, Row }
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamOperatorProto.SampleSimpleRandomInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.UnitSpec
import ksb.csle.component.operator.DataFunSuite
import org.scalatest._
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
 * Test Class for SampleSimpleRandomWihtoutReplacementTest function in data sampling package.
 */

class SampleSimpleRandomWihtoutReplacementTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[Data] = _
  @transient var correctAnswer: Seq[Data] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
      Data(91.5775, 81.572, 84, 73.2035, 79.5918),
      Data(83.4467, 72.9477, 92, 60.6273, 75.1917),
      Data(47.0239, 51.3076, 31, 25.807, 36.0382),
      Data(69.9559, 61.0005, 76, 76.643, 71.2145),
      Data(57.2462, 53.9258, 79, 65.2266, 66.0508),
      Data(42.8488, 46.1728, 7, 31.9797, 28.3842),
      Data(73.7949, 64.0751, 98, 61.2696, 74.4483),
      Data(22.4626, 31.7166, 6, 28.549, 22.0886))
    correctAnswer = Seq(
      Data(83.4467,72.9477,92,60.6273,75.1917),
      Data(47.0239,51.3076,31,25.807,36.0382),
      Data(57.2462,53.9258,79,65.2266,66.0508),
      Data(42.8488,46.1728,7,31.9797,28.3842))
  }

  def convertDF(data: Seq[Data]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  "A SampleSimpleRandomWihtoutReplacement" should "do SRSWOR with EQUAL condition" in {
    val sampleSimpleRandomWithoutReplacementInfo =
      SampleSimpleRandomInfo.newBuilder()
        .setFraction(0.5)
        .setWithReplacement(false)
        .setSeed(7)
        .build
    val operator1 =
      StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.SampleSimpleRandom")
      .setSampleSimpleRandom(sampleSimpleRandomWithoutReplacementInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF(correctAnswer).withColumn("index", row_number.over(w))
    val sampleSimpleRandomWithoutReplacement = RandomSamplingOperator(operator1)
    var desDF = sampleSimpleRandomWithoutReplacement
      .operate(srcDF)
      .withColumn("index", row_number.over(w))
    logger.info(desDF.show.toString)
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
      desDF.col("data1"), correctAnswerDf.col("data1"),
      desDF.col("data2"), correctAnswerDf.col("data2"),
      desDF.col("data3"), correctAnswerDf.col("data3"),
      desDF.col("data4"), correctAnswerDf.col("data4"),
      desDF.col("data5"), correctAnswerDf.col("data5")).collect().foreach {
        case Row(a: Double, b: Double, c: Double, d: Double, e: Integer,
            f: Integer, g: Double, h: Double, i: Double, j: Double) =>
          assert(a === b && c === d && e === f && g === h && i === j,
          "The feature value is not correct after SampleSimpleRandomWihtoutReplacement.")
      }
    logger.info(resultDF.show.toString)
  }
}
