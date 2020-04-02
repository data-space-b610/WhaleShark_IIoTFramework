package ksb.csle.component.operator.transformation

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
import ksb.csle.common.proto.StreamOperatorProto.OneHotEncodingInfo
import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import org.scalactic.TolerantNumerics
import ksb.csle.component.operator.util._
import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.SharedProto._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.linalg.Vector

/**
 * Test Class for OneHot Encoding function in data transformation package.
 */
case class OneHotEncodingData(data1: Int, data2: String)

class OneHotEncodingTest extends UnitSpec with DataFunSuite with Logging {

  @transient var inputData: Seq[OneHotEncodingData] = _
  @transient var correctAnswer: Array[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    inputData = Seq(
      OneHotEncodingData(0, "apple"),
      OneHotEncodingData(1, "banana"),
      OneHotEncodingData(2, "coconut"),
      OneHotEncodingData(1, "banana"),
      OneHotEncodingData(2, "coconut"))
    correctAnswer = Array("(2,[],[])", "(2,[0],[1.0])",
        "(2,[1],[1.0])", "(2,[0],[1.0])", "(2,[1],[1.0])")
  }

  def convertDF(data: Seq[OneHotEncodingData]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataRDD = sc.parallelize(data)
    val df = dataRDD.toDF.repartition(1)
    df
  }

  def convertArrayToDataframeWithIndex(
    input: Array[String],
    name: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df: DataFrame =
    spark.createDataFrame(
        input.zipWithIndex.map{case (value, idx) => (value, idx + 1)})
      .toDF(name, "index")
    df
  }

  "A OneHotEncoding" should
      "do OneHotEncoding with EQUAL condition" in {
    val oneHotEncodingInfo =
      OneHotEncodingInfo.newBuilder()
        .setSelectedColumnName("data2")
        .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)

      .setClsName("ksb.csle.component.operator.ransformation.OneHotEncoding")
      .setOneHotEncoding(oneHotEncodingInfo)
      .build
    val w = Window.partitionBy(lit(1)).orderBy(lit(1))
    val srcDF = convertDF(inputData)
    var correctAnswerDf =
      convertArrayToDataframeWithIndex(correctAnswer, "correct_answer")

    val oneHotEncoding =
      OneHotEncodingOperator(operator1)
    var desDF =
      oneHotEncoding.operate(srcDF)
      .withColumn("index", row_number.over(w))
    val makestring: Vector => String = _.toSparse.toString()
    val stringify = udf(makestring)
    desDF = desDF.withColumn(
        oneHotEncodingInfo.getSelectedColumnName + "Vec",
        stringify(
            desDF(oneHotEncodingInfo.getSelectedColumnName + "Vec")))
    var resultDF = desDF.join(correctAnswerDf,"index")
    resultDF.select(
      desDF.col("data2Vec"), correctAnswerDf.col("correct_answer"))
      .collect().foreach {
        case Row(
          a: String, b: String) => assert(a === b,
          "The feature value is not correct after OneHotEncoding.")
    }
    resultDF.show
  }
}
