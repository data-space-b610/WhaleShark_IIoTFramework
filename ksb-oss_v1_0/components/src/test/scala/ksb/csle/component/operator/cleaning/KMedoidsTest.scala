package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.KMedoidsInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.UnitSpec
import ksb.csle.component.operator.DataFunSuite
import org.scalatest._
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry.FunctionType
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry
import org.apache.log4j.Level
import org.apache.log4j.Logger
/**
 * Test Class for KMedoids function in data clustering package.
 */
case class KMedoidsData(data1: Integer, data2: Integer)

class KMedoidsTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[KMedoidsData] = _
  @transient var correctAnswer: Array[Integer] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        KMedoidsData(2, 6),
        KMedoidsData(3, 4),
        KMedoidsData(3, 8),
        KMedoidsData(4, 7),
        KMedoidsData(6, 2),
        KMedoidsData(6, 4),
        KMedoidsData(7, 3),
        KMedoidsData(7, 4),
        KMedoidsData(8, 5),
        KMedoidsData(7, 6))
    correctAnswer = Array(0, 0, 0, 0, 1, 1, 1, 1, 1, 1)
  }

  def convertDF(data: Seq[KMedoidsData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  def convertArrayToDataframeWithIndex(
    input: Array[Integer],
    name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
      spark.createDataFrame(
          input.zipWithIndex.map{case (value, idx) => (value, idx + 1)})
        .toDF(name, "index")
    df
  }

  "A KMedoids" should "do KMedoids with EQUAL condition" in {
    val kMedoidsInfo = KMedoidsInfo.newBuilder()
      .setKValue(2)
      .setMaxIter(100)
      .setSeed(7)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.leaning.KMedoids")
      .setKMedoids(kMedoidsInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")
    val kMedoids = KMedoidsOperator(operator1)
    var desDF =
      kMedoids.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select("CLUSTER", "correct_answer").collect().foreach{
      case Row(x: Integer, y: Integer) => assert(
          x === y, "The feature value is not correct after KMedoids.")
    }
    resultDF.show
  }
}
