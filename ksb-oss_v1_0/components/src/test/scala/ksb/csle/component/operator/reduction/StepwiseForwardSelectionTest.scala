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
import ksb.csle.common.proto.StreamOperatorProto.StepwiseForwardSelectionInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import ksb.csle.common.base.UnitSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Test Class for StepwiseForwardSelection function in data reduction package.
 */
case class StepwiseForwardSelectionData(
    label: Double, a1: Double, a2: Double,
    a3: Double, a4: Double, a5: Double)
case class StepwiseForwardSelectionCorrectData(
    label: Double, a2: Double)

class StepwiseForwardSelectionTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[StepwiseForwardSelectionData] = _
  @transient var correctAnswer: Seq[StepwiseForwardSelectionCorrectData] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        StepwiseForwardSelectionData(88.01632464, 0.636574028, 8.442168148,
            2.116099574, 2.771867529, 8.448426549),
        StepwiseForwardSelectionData(138.7548543, 4.595454155, 4.388041662,
            4.926093835, 2.682403955, 8.617751819),
        StepwiseForwardSelectionData(38.28387155, 4.292060076, 0.861090232,
            9.15798394, 6.607498138, 4.084605081),
        StepwiseForwardSelectionData(82.14778398, 8.559949469, 3.856237673,
            1.0381353, 1.687798024, 2.8930632),
        StepwiseForwardSelectionData(33.79002054, 2.272320698, 1.834078721,
            6.300620501, 1.93796937, 2.069888374),
        StepwiseForwardSelectionData(7.38866956, 3.956280442, 0.34429275,
            4.337370526, 7.563450458, 8.591228627),
        StepwiseForwardSelectionData(191.4532705, 2.220060635, 7.843024669,
            6.462687043, 8.731893832, 9.28188111),
        StepwiseForwardSelectionData(15.34906002, 6.105141656, 1.074919676,
            1.162823303, 9.241172289, 7.23885872),
        StepwiseForwardSelectionData(97.40527838, 2.691071171, 3.571957641,
            7.805965771, 6.843146634, 3.053870888),
        StepwiseForwardSelectionData(111.4973958, 4.262579387, 7.369277727,
            0.820670523, 6.687792013, 3.976156809))
    correctAnswer = Seq(
        StepwiseForwardSelectionCorrectData(88.01632464, 8.442168148),
        StepwiseForwardSelectionCorrectData(138.7548543, 4.388041662),
        StepwiseForwardSelectionCorrectData(38.28387155, 0.861090232),
        StepwiseForwardSelectionCorrectData(82.14778398, 3.856237673),
        StepwiseForwardSelectionCorrectData(33.79002054, 1.834078721),
        StepwiseForwardSelectionCorrectData(7.38866956, 0.34429275),
        StepwiseForwardSelectionCorrectData(191.4532705, 7.843024669),
        StepwiseForwardSelectionCorrectData(15.34906002, 1.074919676),
        StepwiseForwardSelectionCorrectData(97.40527838, 3.571957641),
        StepwiseForwardSelectionCorrectData(111.4973958, 7.369277727))
  }

  def convertDF(data: Seq[StepwiseForwardSelectionData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def convertDF2(data: Seq[StepwiseForwardSelectionCorrectData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  "A StepwiseForwardSelection" should
      "do StepwiseForwardSelection with EQUAL condition" in {
    val stepwiseForwardSelectionInfo =
      StepwiseForwardSelectionInfo.newBuilder()
        .setLabelName("label")
        .setPValue(0.01)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.StepwiseForwardSelection")
      .setStepwiseForwardSelection(stepwiseForwardSelectionInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF2(correctAnswer).withColumn("index", row_number.over(w))
    val stepwiseForwardSelection = StepwiseForwardSelectOperator(operator1)
    var desDF =
      stepwiseForwardSelection.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("label"), correctAnswerDf.col("label"),
        desDF.col("a2"), correctAnswerDf.col("a2"))
      .collect().foreach {
        case Row(
            a: Double, b: Double,
            c: Double, d: Double) => assert(a === b && c === d,
            "The feature value is not correct after StepwiseForwardSelection.")
      }
    resultDF.show
  }
}
