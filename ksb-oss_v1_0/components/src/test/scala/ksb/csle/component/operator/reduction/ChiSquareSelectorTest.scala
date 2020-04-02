package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame,SQLContext, Row}
import org.apache.logging.log4j.scala.Logging
import org.apache.log4j.Level
import org.apache.log4j.Logger
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import ksb.csle.component.operator.util.CalculatorUtils
import ksb.csle.common.proto.StreamOperatorProto.ChiSquareSelectorInfo
import ksb.csle.common.proto.SharedProto._

/**
 * Test Class for ChiSquareSelector function in data reduction package.
 */
case class ChiSquareSelectorData(
    label: Double, a1: Double, a2: Double, a3: Double, a4: Double)
case class ChiSquareSelectorCorrectData(
    a3: Double, a4: Double)

class ChiSquareSelectorTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[ChiSquareSelectorData] = _
  @transient var correctAnswer: Seq[ChiSquareSelectorCorrectData] = _

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        ChiSquareSelectorData(1.0, 0.0, 0.0, 18.0, 1.0),
        ChiSquareSelectorData(0.0, 0.0, 1.0, 12.0, 0.0),
        ChiSquareSelectorData(0.0, 1.0, 0.0, 15.0, 0.1))
    correctAnswer = Seq(
        ChiSquareSelectorCorrectData(18.0, 1.0),
        ChiSquareSelectorCorrectData(12.0, 0.0),
        ChiSquareSelectorCorrectData(15.0, 0.1))
  }

  def convertDF(data: Seq[ChiSquareSelectorData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  def convertDF2(
      data: Seq[ChiSquareSelectorCorrectData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  "A ChiSquareSelector" should
      "do ChiSquareSelector with EQUAL condition" in {
    val chiSquareSelectorInfo =
      ChiSquareSelectorInfo.newBuilder()
        .setLabelName("label")
        .setNumTopFeatures(2)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.eduction.ChiSquareSelector")
      .setChiSquareSelector(chiSquareSelectorInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF2(correctAnswer).withColumn("index", row_number.over(w))
    val chiSquareSelector =
      ChiSquareSelectOperator(operator1)
    var desDF =
      chiSquareSelector.operate(srcDF)
        .withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("a3"), correctAnswerDf.col("a3"),
        desDF.col("a4"), correctAnswerDf.col("a4"))
      .collect().foreach {
        case Row(
            a: Double, b: Double,
            c: Double, d: Double) => assert(a === b && c === d,
            "The feature value is not correct after " +
            "ChiSquareSelector.")
      }
    resultDF.show
  }
}
