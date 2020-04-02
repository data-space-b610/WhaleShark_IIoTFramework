package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.sql._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.base.UnitSpec

class StringIndexOperatorTest extends UnitSpec with Logging {
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("learn spark")
    .getOrCreate()

  "StringIndexOperator" should "index a column" in {
    import spark.implicits._

    val inputs = Seq(
        (1, "korea", 0.1),
        (2, "france", 0.2),
        (3, null, 0.3),
        (4, null, 0.4),
        (5, "", 0.5),
        (6, "", 0.6),
        (7, "america", 0.7)).toDF("id", "city", "score")

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.transformation.StringIndexOperator")
      .setStringIndex(StringIndexInfo.newBuilder()
          .setSrcColumnName("city")
          .setDestColumnName("ix_city"))
      .build()
    val op = new StringIndexOperator(info)
    val r = op.operate(inputs)

    r.printSchema()
    r.show()
    r.schema("ix_city")
  }
}
