package ksb.csle.component.operator.reduction

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamOperatorProto.SampleStratifiedInfo
import ksb.csle.common.proto.StreamOperatorProto.FractionFieldEntry
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
 * Test Class for SimpleRandomSamplingWihtoutReplacementTest function in data sampling package.
 */
case class SampleStratifiedData(data1: String, data2: String)

class SampleStratifiedTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[SampleStratifiedData] = _
  @transient var correctAnswer: Seq[SampleStratifiedData] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
      SampleStratifiedData("1", "a"),
      SampleStratifiedData("1", "b"),
      SampleStratifiedData("2", "c"),
      SampleStratifiedData("2", "d"),
      SampleStratifiedData("2", "e"),
      SampleStratifiedData("3", "f"))
    correctAnswer = Seq(
      SampleStratifiedData("1", "b"),
      SampleStratifiedData("2", "c"),
      SampleStratifiedData("2", "e"))
  }

  def convertDF(data: Seq[SampleStratifiedData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  "A SampleStratified" should "do SampleStratified with EQUAL condition" in {
    val sampleStratifiedInfo = SampleStratifiedInfo.newBuilder()
      .setSelectedColumnId(0)
      .addFractions(
        FractionFieldEntry.newBuilder()
          .setKey("1")
          .setValue(0.1)
          .build())
       .addFractions(
        FractionFieldEntry.newBuilder()
          .setKey("2")
          .setValue(0.6)
          .build())
       .addFractions(
        FractionFieldEntry.newBuilder()
          .setKey("3")
          .setValue(0.3)
          .build())
      .setWithReplacement(false)
      .setSeed(7)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.SampleStratified")
      .setSampleStratified(sampleStratifiedInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF(correctAnswer).withColumn("index", row_number.over(w))
    val sampleStratified = StratifiedSamplingOperator(operator1)
    var desDF = sampleStratified.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("data1"), correctAnswerDf.col("data1"),
        desDF.col("data2"), correctAnswerDf.col("data2"))
      .collect().foreach {
        case Row(a: String, b: String, c: String, d: String) =>
          assert(a === b && c === d, "The feature value is not correct after SampleStratified.")
    }
    resultDF.show
  }
}
