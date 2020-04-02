package ksb.csle.component.operator.analysis

import org.apache.log4j._

import org.apache.spark.sql._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto.RandomForestClassifierPredictInfo

object RandomForestClassifierPredictOperatorTest {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("learn spark")
      .getOrCreate()

    test1(spark)
  }

  def test1(spark: SparkSession) {
    val inputs = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/tmp/ecg/data/ecg_features_strlbl_train.csv")

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.analysis.RandomForestClassifierPredictOperator")
      .setRandomForestClassifierPredictInfo(
          RandomForestClassifierPredictInfo.newBuilder()
            .setModelBasePath("/tmp/ecg/model"))
      .build()

    val op = new RandomForestClassifierPredictOperator(info)
    val result = op.operate(inputs)
    result.printSchema()
    result.show(5)
  }
}
