package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.sql._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._

import ksb.csle.common.base.UnitSpec

class VectorizeColumnOperatorTest extends UnitSpec with Logging {
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("learn spark")
    .getOrCreate()

  "VectorizeColumnOperator" should "vectorize source columns" in {
    import spark.implicits._

    val inputs = Seq(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)).toDF("c1", "c2", "c3")

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.transformation.VectorizeColumnOperator")
      .setVectorizeColumn(VectorizeColumnInfo.newBuilder()
          .setSrcColumnNames("*")
          .setDestColumnName("features")
          .setKeepSrcColumns(true))
      .build()
    val op = new VectorizeColumnOperator(info)
    val r = op.operate(inputs)

    r.printSchema()
    r.show()
    r.schema("features")
  }

  "VectorizeColumnOperator" should "drop source columns" in {
    import spark.implicits._

    val inputs = Seq(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)).toDF("c1", "c2", "c3")

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.transformation.VectorizeColumnOperator")
      .setVectorizeColumn(VectorizeColumnInfo.newBuilder()
          .setSrcColumnNames("*")
          .setDestColumnName("features")
          .setKeepSrcColumns(false))
      .build()
    val op = new VectorizeColumnOperator(info)
    val r = op.operate(inputs)

    r.printSchema()
    r.show()
    Try {
      r.schema("c1")
      r.schema("c2")
      r.schema("c3")
    } match {
      case Success(_) => false
      case Failure(_) => true
    }
  }
}
