package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.sql._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._

import ksb.csle.common.base.UnitSpec
import ksb.csle.common.proto.StreamOperatorProto.RenamingParam

class RenameColumnOperatorTest extends UnitSpec with Logging {
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("learn spark")
    .getOrCreate()

  "RenameColumnOperator" should "rename a column" in {
    import spark.implicits._

    val inputs = Seq(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)).toDF("c1", "c2", "c3")

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.transformation.RenameColumnOperator")
      .setRenameColumn(RenameColumnInfo.newBuilder()
          .setExistingName("c1")
          .setNewName("renamed_c1"))
      .build()
    val op = new RenameColumnOperator(info)
    val r = op.operate(inputs)

    r.printSchema()
    r.show()
    r.schema("renamed_c1")
  }

  "RenameColumnOperator" should "rename multiple columns" in {
    import spark.implicits._

    val inputs = Seq(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)).toDF("c1", "c2", "c3")

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.transformation.RenameColumnOperator")
      .setRenameColumn(RenameColumnInfo.newBuilder()
          .setExistingName("c1")
          .setNewName("renamed_c1")
          .addMore(RenamingParam.newBuilder()
              .setExistingName("c2")
              .setNewName("renamed_c2"))
          .addMore(RenamingParam.newBuilder()
              .setExistingName("c3")
              .setNewName("renamed_c3")))
      .build()
    val op = new RenameColumnOperator(info)
    val r = op.operate(inputs)

    r.printSchema()
    r.show()
    r.schema("renamed_c1")
    r.schema("renamed_c2")
    r.schema("renamed_c3")
  }
}
