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
import ksb.csle.common.proto.StreamOperatorProto.AggregateInfo
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry.FunctionType
import ksb.csle.common.proto.StreamOperatorProto.AttributeEntry
import ksb.csle.common.proto.SharedProto._

/**
 * Test Class for Aggregate function in data reduction package.
 */
case class AggregationData(
    play: String, outlook: String, temperature: Long,
    humidity: Long, wind: Boolean)
case class AggregationCorrectData(
    outlook: String, play: String, temperature_avg: Double,
    humidity_sum: Long, outlook_concat: String, temperature_count: Long)

class AggregateTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[AggregationData] = _
  @transient var correctAnswer: Seq[AggregationCorrectData] = _
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
        AggregationData("no", "sunny", 85, 85, false),
        AggregationData("no", "sunny", 80, 90, true),
        AggregationData("yes", "overcast", 83, 78, false),
        AggregationData("yes", "rain", 70, 96, false),
        AggregationData("yes", "rain", 68, 80, false),
        AggregationData("no", "rain", 65, 70, true),
        AggregationData("yes", "overcast", 64, 65, true),
        AggregationData("no", "sunny", 72, 95, false),
        AggregationData("yes", "sunny", 69, 70, false),
        AggregationData("yes", "rain", 75, 80, false),
        AggregationData("yes", "sunny", 75, 70, true),
        AggregationData("yes", "overcast", 72, 90, true),
        AggregationData("yes", "overcast", 81, 75, false),
        AggregationData("no", "rain", 71, 80, true))
    correctAnswer = Seq(
        AggregationCorrectData("overcast", "yes", 75, 308,
            "overcast|overcast|overcast|overcast", 4),
        AggregationCorrectData("rain", "no", 68, 150, "rain|rain", 2),
        AggregationCorrectData("rain", "yes", 71, 256, "rain|rain|rain", 3),
        AggregationCorrectData("sunny", "no", 79, 270, "sunny|sunny|sunny", 3),
        AggregationCorrectData("sunny", "yes", 72, 140, "sunny|sunny", 2))
  }

  def convertDF(data: Seq[AggregationData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  def convertDF2(data: Seq[AggregationCorrectData]) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF
    df
  }

  "A Aggregate" should "do Aggregate with EQUAL condition" in {
    val aggregateInfo =
      AggregateInfo.newBuilder()
        .addAttributes(
            AttributeEntry.newBuilder()
              .setAttributeName("temperature")
              .setFunctionType(FunctionType.AVERAGE)
              .build())
        .addAttributes(
            AttributeEntry.newBuilder()
              .setAttributeName("humidity")
              .setFunctionType(FunctionType.SUM)
              .build())
        .addAttributes(
            AttributeEntry.newBuilder()
              .setAttributeName("outlook")
              .setFunctionType(FunctionType.CONCATENATION)
              .build())
        .addAttributes(
            AttributeEntry.newBuilder()
              .setAttributeName("temperature")
              .setFunctionType(FunctionType.COUNT)
              .build())
        .addGroupByAttributeName("outlook")
        .addGroupByAttributeName("play")
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.Aggregate")
      .setAggregate(aggregateInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertDF2(correctAnswer).withColumn("index", row_number.over(w))
    val aggregation = AggregateOperator(operator1)
    var desDF =
      aggregation.operate(srcDF).withColumn("index", row_number.over(w))
    var resultDF = desDF.join(correctAnswerDf,"index").sort(asc("index"))
    resultDF.select(
        desDF.col("outlook"), correctAnswerDf.col("outlook"),
        desDF.col("play"), correctAnswerDf.col("play"),
        desDF.col("temperature_avg"), correctAnswerDf.col("temperature_avg"),
        desDF.col("humidity_sum"), correctAnswerDf.col("humidity_sum"),
        desDF.col("outlook_concat"), correctAnswerDf.col("outlook_concat"),
        desDF.col("temperature_count"),
        correctAnswerDf.col("temperature_count"))
      .collect().foreach {
        case Row(
            a: String, b: String,
            c: String, d: String,
            e: Double, f: Double,
            g: Long, h: Long,
            i: String, j: String,
            k: Long, l: Long) => assert(
                a === b && c === d && e === f && g === h && i === j && k === l,
                "The feature value is not correct after Aggregation.")
        }
    resultDF.show
  }
}
