package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.GeneralizedLinearRegressionInfo
import ksb.csle.common.proto.StreamOperatorProto.GeneralizedLinearRegressionInfo._
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
 * Test Class for GeneralizedLinearRegression function in data clustering package.
 */
case class GeneralizedLinearRegressionData(
    label: Double, a1: Double, a2: Double,
    a3: Double, a4: Double, a5: Double)
case class GeneralizedLinearRegressionCorrectData(
    attribute: String, weight: Double)

class GeneralizedLinearRegressionTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[GeneralizedLinearRegressionData] = _
  @transient var correctAnswer: Seq[GeneralizedLinearRegressionCorrectData] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        GeneralizedLinearRegressionData(88.01632464, 0.636574028, 8.442168148,
            2.116099574, 2.771867529, 8.448426549),
        GeneralizedLinearRegressionData(138.7548543, 4.595454155, 4.388041662,
            4.926093835, 2.682403955, 8.617751819),
        GeneralizedLinearRegressionData(38.28387155, 4.292060076, 0.861090232,
            9.15798394, 6.607498138, 4.084605081),
        GeneralizedLinearRegressionData(82.14778398, 8.559949469, 3.856237673,
            1.0381353, 1.687798024, 2.8930632),
        GeneralizedLinearRegressionData(33.79002054, 2.272320698, 1.834078721,
            6.300620501, 1.93796937, 2.069888374),
        GeneralizedLinearRegressionData(7.38866956, 3.956280442, 0.34429275,
            4.337370526, 7.563450458, 8.591228627),
        GeneralizedLinearRegressionData(191.4532705, 2.220060635, 7.843024669,
            6.462687043, 8.731893832, 9.28188111),
        GeneralizedLinearRegressionData(15.34906002, 6.105141656, 1.074919676,
            1.162823303, 9.241172289, 7.23885872),
        GeneralizedLinearRegressionData(97.40527838, 2.691071171, 3.571957641,
            7.805965771, 6.843146634, 3.053870888),
        GeneralizedLinearRegressionData(111.4973958, 4.262579387, 7.369277727,
            0.820670523, 6.687792013, 3.976156809))
    correctAnswer = Seq(
        GeneralizedLinearRegressionCorrectData("a1", 0.20572860485369465),
        GeneralizedLinearRegressionCorrectData("a2", 0.3212522380884287),
        GeneralizedLinearRegressionCorrectData("a3", 0.19816273370623586),
        GeneralizedLinearRegressionCorrectData("a4", -0.056268314439480556),
        GeneralizedLinearRegressionCorrectData("a5", 0.005911216023548133))
  }

  def convertDF(data: Seq[GeneralizedLinearRegressionData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def convertDF2(data: Seq[GeneralizedLinearRegressionCorrectData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  "A GeneralizedLinearRegression" should "do GeneralizedLinearRegression with EQUAL condition" in {
    val generalizedlinearRegressionInfo =
      GeneralizedLinearRegressionInfo.newBuilder()
        .setLabelName("label")
        .setFamilyType(FamilyType.GAUSSIAN)
        .setLinkType(LinkType.LOG)
        .setMaxIter(10)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.leaning.GeneralizedLinearRegression")
      .setGeneralizedLinearRegression(generalizedlinearRegressionInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF2(correctAnswer).withColumn("index", row_number.over(w))
    val generalizedlinearRegression = GeneralizedLinearRegressionOperator(operator1)
    var desDF =
      generalizedlinearRegression.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("attribute"), correctAnswerDf.col("attribute"),
        desDF.col("weight"), correctAnswerDf.col("weight"))
      .collect().foreach {
        case Row(
            a: String, b: String,
            c: Double, d: Double) => assert(a === b && c === d,
            "The feature value is not correct after GeneralizedLinearRegression.")
      }
    resultDF.show
  }
}
