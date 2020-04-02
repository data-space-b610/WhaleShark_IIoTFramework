package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame,SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.log4j.Level
import org.apache.log4j.Logger
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.SharedProto._

/**
 * Test Class for Filtering function in data cleaning package.
 */
case class WordCount(word: String, count: Int)

class FilterTest extends UnitSpec with DataFunSuite with Logging {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  @transient var wordcounts: Seq[WordCount] = _
  @transient var wordCountsDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    wordcounts = Seq(
      WordCount("I", 1), WordCount("am", 1),
      WordCount("so", 3), WordCount("happy", 4),
      WordCount("now", 2), WordCount("are", 3),
      WordCount("you", 2), WordCount("happy", 6),
      WordCount("now", 8), WordCount("I", 1),
      WordCount("I", 3), WordCount("I", 10))
    wordCountsDF = convertDF()
  }

  def convertDF() : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val wordCountsRDD = sc.parallelize(wordcounts, 4)
    val df = wordCountsRDD.toDF
    df
  }

  "A Filter" should "do filtering with LESS_THAN condition" in {
    val filterInfo = FilterInfo.newBuilder()
      .setColName("count")
      .setCondition(FilterInfo.Condition.LESS_THAN)
      .setValue(8)
      .build

    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.eduction.Filter")
      .setFilter(filterInfo)
      .build
    val filter = FilterOperator(operator1)
    val resultDF = filter.operate(wordCountsDF)
    assert(resultDF.count === 10)
  }

  it should "do filtering with EQAUL condition" in {
    val filterInfo = FilterInfo.newBuilder()
      .setColName("count")
      .setCondition(FilterInfo.Condition.EQUAL)
      .setValue(1)
      .build

    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(0)
      .setClsName("ksb.csle.component.operator.eduction.Filter")
      .setFilter(filterInfo)
      .build
    val filter = FilterOperator(operator1)
    val resultDF = filter.operate(wordCountsDF)
    assert(resultDF.count === 3)
  }
}
