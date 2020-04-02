package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.KMeansInfo
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
 * Test Class for KMeans function in data clustering package.
 */
case class Data(data1: Double, data2: Double, data3: Int, data4: Double, data5: Double)

class KMeansTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[Data] = _
  @transient var correctAnswer: Array[Integer] = _
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
    correctAnswer = Array(0, 0, 1, 0, 0, 1, 0, 1)
  }

  def convertDF(data: Seq[Data]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def convertArrayToDataframeWithIndex(
      input: Array[Integer],
      name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
      spark.createDataFrame(input.zipWithIndex.map {
        case (value, idx) => (value, idx + 1)}).toDF(name, "index")
    df
  }

  def convertArrayToDataframeWithIndex(
      input: Array[String],
      name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
      spark.createDataFrame(input.zipWithIndex.map {
        case (value, idx) => (value, idx + 1)}).toDF(name, "index")
    df
  }

  "A KMeans" should "do KMeans with EQUAL condition" in {
    val kMeansInfo = KMeansInfo.newBuilder()
      .setKValue(2)
      .setMaxIterations(100)
      .setMaxRuns(10)
      .setSeed(7)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.leaning.K_Means")
      .setKMeans(kMeansInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")
    val kMeans = KMeansOperator(operator1)
    var desDF = kMeans.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index")
    resultDF.select("CLUSTER", "correct_answer").collect().foreach{
      case Row(x: Integer, y: Integer) =>
        assert(x === y, "The feature value is not correct after KMeans.")
    }
    resultDF.show
  }
}
