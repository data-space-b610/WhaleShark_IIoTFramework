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
import ksb.csle.common.proto.StreamOperatorProto.PrincipalComponentAnalysisInfo
import ksb.csle.common.proto.SharedProto._

/**
 * Test Class for PrincipalComponentAnalysis function in data reduction package.
 */
case class PrincipalComponentAnalysisData(
    a1: Double, a2: Double, a3: Double, a4: Double)
case class PrincipalComponentAnalysisCorrectData(
    pc1: Double, pc2: Double)

class PrincipalComponentAnalysisTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[PrincipalComponentAnalysisData] = _
  @transient var correctAnswer: Seq[PrincipalComponentAnalysisCorrectData] = _

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

   override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        PrincipalComponentAnalysisData(5.1, 3.5, 1.4, 0.2),
        PrincipalComponentAnalysisData(4.9, 3.0, 1.4, 0.2),
        PrincipalComponentAnalysisData(4.7, 3.2, 1.3, 0.2),
        PrincipalComponentAnalysisData(4.6, 3.1, 1.5, 0.2),
        PrincipalComponentAnalysisData(5.0, 3.6, 1.4, 0.2),
        PrincipalComponentAnalysisData(5.4, 3.9, 1.7, 0.4),
        PrincipalComponentAnalysisData(4.6, 3.4, 1.4, 0.3),
        PrincipalComponentAnalysisData(5.0, 3.4, 1.5, 0.2),
        PrincipalComponentAnalysisData(4.4, 2.9, 1.4, 0.2),
        PrincipalComponentAnalysisData(4.9, 3.1, 1.5, 0.1))
    correctAnswer = Seq(
        PrincipalComponentAnalysisCorrectData(
            -0.2860, 0.0469),
        PrincipalComponentAnalysisCorrectData(
            0.2059, 0.2269),
        PrincipalComponentAnalysisCorrectData(
            0.2126, -0.0547),
        PrincipalComponentAnalysisCorrectData(
            0.3182, -0.0373),
        PrincipalComponentAnalysisCorrectData(
            -0.2908, -0.0879),
        PrincipalComponentAnalysisCorrectData(
            -0.8452, -0.0168),
        PrincipalComponentAnalysisCorrectData(
            0.1082, -0.2691),
        PrincipalComponentAnalysisCorrectData(
            -0.1639, 0.0524),
        PrincipalComponentAnalysisCorrectData(
            0.6116, -0.0619),
        PrincipalComponentAnalysisCorrectData(
            0.1292, 0.2017))
  }

  def convertDF(data: Seq[PrincipalComponentAnalysisData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def convertDF2(
      data: Seq[PrincipalComponentAnalysisCorrectData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def getRound(a: Double , n: Int): Double = {
    Math.round(a * Math.pow(10, n)) / Math.pow(10, n)
  }

  "A PrincipalComponentAnalysis" should
      "do PrincipalComponentAnalysis with EQUAL condition" in {
    val principalComponentAnalysisInfo =
      PrincipalComponentAnalysisInfo.newBuilder()
        .setKValue(2)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.PrincipalComponentAnalysis")
      .setPrincipalComponentAnalysis(principalComponentAnalysisInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF2(correctAnswer).withColumn("index", row_number.over(w))
    val principalComponentAnalysis = PCAOperator(operator1)
    var desDF =
      principalComponentAnalysis.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("pc1"), correctAnswerDf.col("pc1"),
        desDF.col("pc2"), correctAnswerDf.col("pc2"))
      .collect().foreach {
        case Row(
            a: Double, b: Double,
            c: Double, d: Double) => assert(getRound(a, 3) === getRound(b, 3)
                && getRound(c, 3) === getRound(d, 3),
            "The feature value is not correct after PCA.")
      }
    resultDF.show
  }
}
