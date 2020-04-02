package ksb.csle.component.operator.cleaning

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.StreamOperatorProto.EMClusteringInfo
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
 * Test Class for EMClustering function in data clustering package.
 */
case class EMClusteringData(
    a1: Double, a2: Double, a3: Double, a4: Double)

class EMClusteringTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[EMClusteringData] = _
  @transient var correctAnswer: Array[Integer] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        EMClusteringData(5.1, 3.5, 1.4, 0.2),
        EMClusteringData(4.9, 3.0, 1.4, 0.2),
        EMClusteringData(4.7, 3.2, 1.3, 0.2),
        EMClusteringData(4.6, 3.1, 1.5, 0.2),
        EMClusteringData(5.0, 3.6, 1.4, 0.2),
        EMClusteringData(5.4, 3.9, 1.7, 0.4),
        EMClusteringData(4.6, 3.4, 1.4, 0.3),
        EMClusteringData(5.0, 3.4, 1.5, 0.2),
        EMClusteringData(4.4, 2.9, 1.4, 0.2),
        EMClusteringData(4.9, 3.1, 1.5, 0.1),
        EMClusteringData(5.4, 3.7, 1.5, 0.2),
        EMClusteringData(4.8, 3.4, 1.6, 0.2),
        EMClusteringData(4.8, 3.0, 1.4, 0.1),
        EMClusteringData(4.3, 3.0, 1.1, 0.1),
        EMClusteringData(5.8, 4.0, 1.2, 0.2))
    correctAnswer = Array(0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1)
  }

  def convertDF(data: Seq[EMClusteringData]): DataFrame = {
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

  "A EMClustering" should
      "do EMClustering with EQUAL condition" in {
    val eMClusteringInfo =
      EMClusteringInfo.newBuilder()
        .setKValue(2)
        .setMaxIter(100)
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.leaning.EMClustering")
      .setEMClustering(eMClusteringInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")
    val eMClustering = EMClusteringOperator(operator1)
    var desDF =
      eMClustering.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select("CLUSTER", "correct_answer").collect().foreach{
      case Row(x: Integer, y: Integer) => assert(
          x === y, "The feature value is not correct after EMClustering.")
    }
    resultDF.show
  }
}
