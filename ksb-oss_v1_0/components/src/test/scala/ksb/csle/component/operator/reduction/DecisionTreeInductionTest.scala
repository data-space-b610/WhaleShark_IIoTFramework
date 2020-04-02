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
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo
import ksb.csle.common.proto.StreamOperatorProto.DecisionTreeInductionInfo.ImpurityType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import ksb.csle.common.base.UnitSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Test Class for DecisionTreeInduction function in data reduction package.
 */
case class DecisionTreeInductionData(
    play: String, outlook: String, temperature: Int,
    humidity: Int, wind: Boolean)
case class DecisionTreeInductionCorrectData(
    outlook: String, temperature: Int, wind: Boolean)

class DecisionTreeInductionTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[DecisionTreeInductionData] = _
  @transient var correctAnswer: Seq[DecisionTreeInductionCorrectData] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        DecisionTreeInductionData("no", "sunny", 85, 85, false),
        DecisionTreeInductionData("no", "sunny", 80, 90, true),
        DecisionTreeInductionData("yes", "overcast", 83, 78, false),
        DecisionTreeInductionData("yes", "rain", 70, 96, false),
        DecisionTreeInductionData("yes", "rain", 68, 80, false),
        DecisionTreeInductionData("no", "rain", 65, 70, true),
        DecisionTreeInductionData("yes", "overcast", 64, 65, true),
        DecisionTreeInductionData("no", "sunny", 72, 95, false),
        DecisionTreeInductionData("yes", "sunny", 69, 70, false),
        DecisionTreeInductionData("yes", "rain", 75, 80, false),
        DecisionTreeInductionData("yes", "sunny", 75, 70, true),
        DecisionTreeInductionData("yes", "overcast", 72, 90, true),
        DecisionTreeInductionData("yes", "overcast", 81, 75, false),
        DecisionTreeInductionData("no", "rain", 71, 80, true))
    correctAnswer = Seq(
        DecisionTreeInductionCorrectData("sunny", 85, false),
        DecisionTreeInductionCorrectData("sunny", 80, true),
        DecisionTreeInductionCorrectData("overcast", 83, false),
        DecisionTreeInductionCorrectData("rain", 70, false),
        DecisionTreeInductionCorrectData("rain", 68, false),
        DecisionTreeInductionCorrectData("rain", 65, true),
        DecisionTreeInductionCorrectData("overcast", 64, true),
        DecisionTreeInductionCorrectData("sunny", 72, false),
        DecisionTreeInductionCorrectData("sunny", 69, false),
        DecisionTreeInductionCorrectData("rain", 75, false),
        DecisionTreeInductionCorrectData("sunny", 75, true),
        DecisionTreeInductionCorrectData("overcast", 72, true),
        DecisionTreeInductionCorrectData("overcast", 81, false),
        DecisionTreeInductionCorrectData("rain", 71, true))
  }

  def convertDF(data: Seq[DecisionTreeInductionData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def convertDF2(
      data: Seq[DecisionTreeInductionCorrectData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

 "A DecisionTreeInduction" should
      "do DecisionTreeInduction with EQUAL condition" in {
    val decisionTreeInductionInfo =
      DecisionTreeInductionInfo.newBuilder()
        .setLabelName("play")
        .setMaxDepth(20)
        .setMinInfoGain(0.1d)
        .setMinInstancesPerNode(2)
        .setMaxBins(32)
        .setCacheNodeIds(true)
        .setCheckpointInterval(-1)
        .setImpurityType(ImpurityType.GINI)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.DecisionTreeInduction")
      .setDecisionTreeInduction(decisionTreeInductionInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF2(correctAnswer).withColumn("index", row_number.over(w))
    val decisionTreeInduction = DecisionTreeInductOperator(operator1)
    var desDF =
      decisionTreeInduction.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("outlook"), correctAnswerDf.col("outlook"),
        desDF.col("temperature"), correctAnswerDf.col("temperature"),
        desDF.col("wind"), correctAnswerDf.col("wind"))
      .collect().foreach {
        case Row(
            a: String, b: String,
            c: Int, d: Int,
            e: Boolean, f: Boolean) => assert(a === b && c === d && e === f,
            "The feature value is not correct after DecisionTreeInduction.")
      }
    resultDF.show
  }
}
