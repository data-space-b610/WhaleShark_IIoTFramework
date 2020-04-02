package ksb.csle.component.operator.integration

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
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.functions.{col, lit, when}
import ksb.csle.common.base.UnitSpec

/**
 * Test Class for OrderByColumn function in data integration package.
 */
class OrderByColumnTest extends UnitSpec with DataFunSuite with Logging {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  @transient var data: Seq[Integer] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = Seq(1, 3, 10, 12, 5, 2, 8, 13, 9)
  }

  "A OrderByColumn" should "do ordering with DESC condition" in {
    val orderByColInfo = OrderByColumnInfo.newBuilder()
      .setSelectedColumnId(0)
      .setMethod(OrderByColumnInfo.Method.DESC)
      .build()
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val originDf = data.toDF("original")
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(0)
      .setClsName("ksb.csle.component.operator.ntegration.OrderByColumn")
      .setOrderByColumn(orderByColInfo)
      .build
    val orderByColumn = OrderByOperator(operator1)
    var originResultDf = orderByColumn.operate(originDf)
    originResultDf = originResultDf.withColumn(
        "index", row_number.over(Window.partitionBy(lit(1)).orderBy(lit(1))))
    var expectedDf = data.sortWith(_>_).toDF("expected")
    expectedDf =  expectedDf.withColumn(
        "index", row_number.over(Window.partitionBy(lit(1)).orderBy(lit(1))))
    val resultDF = originResultDf.join(
        expectedDf, originResultDf.col("index") === expectedDf.col("index"))
    resultDF.select("original","expected").collect().foreach {
      case Row(x: Integer, y: Integer) =>
        assert(x === y, "The feature value is not correct after OrderByColumn.")
    }
  }
}
