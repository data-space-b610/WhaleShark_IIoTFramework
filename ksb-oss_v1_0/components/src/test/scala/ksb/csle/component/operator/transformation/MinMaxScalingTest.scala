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
import ksb.csle.common.proto.StreamOperatorProto._
import org.scalatest._
import org.scalatest.Assertions._
import ksb.csle.component.operator.DataFunSuite
import org.scalactic.TolerantNumerics
import ksb.csle.component.operator.util._
import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.SharedProto._

/**
 * Test Class for MinMax Scaling function in data transformation package.
 */
class MinMaxScalingTest extends UnitSpec with DataFunSuite with Logging {

  @transient var original: Seq[Double] = _
  @transient var df: DataFrame = _
  @transient var expected: Seq[Double]  = _
  implicit val doubleEq: org.scalactic.Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-4f)

  override def beforeAll(): Unit = {
    super.beforeAll()
    original = Seq(0,1,2,3,4)
    expected = Seq(-1.000000001,-0.5,0,0.5,1.0)
    df = spark.createDataFrame(original.zip(expected)).toDF("original", "expected")
  }

  "A MinMaxScaling" should "do scaling with condition" in {
    val maxMinScalingInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(0)
      .setMax("1")
      .setMin("-1")
      .setMaxRealValue("1")
      .setMinRealValue("-1")
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(1)
      .setClsName("ksb.csle.component.operator.ransformation.MinMaxScaling")
      .setMinMaxScaling(maxMinScalingInfo)
      .build
    val maxMinScaling = MinMaxScalingOperator(operator1)
    val resultDF = maxMinScaling.operate(df)
    resultDF.select("expected", "original").collect().foreach {
      case Row(r1: Double, r2: Double) =>
        assert(r1 === r2, "original is different with expected.")}
  }
}
