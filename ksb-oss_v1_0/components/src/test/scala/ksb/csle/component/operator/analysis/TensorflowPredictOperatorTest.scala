package ksb.csle.component.operator.analysis

import scala.util.parsing.json.JSONObject

import org.apache.spark.sql._
import org.apache.spark.ml.linalg.Vectors

import ksb.csle.component.writer.KafkaWriter
import ksb.csle.common.proto.StreamOperatorProto.TensorflowPredictorInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.DatasourceProto.KafkaInfo
import ksb.csle.common.proto.DatasourceProto.StreamWriterInfo

object TensorflowPredictOperatorTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("learn spark")
      .getOrCreate()

//    testEnergyEecArrayInput(spark)
//    testEnergyEecVectorInput(spark)

    testMnistArrayInput(spark)
  }

  private def testMnistArrayInput(spark: SparkSession) {
    import spark.implicits._

    val seq = Seq(
        (Array.fill[Double](784)(0).toSeq))
    val df = seq.toDF("images")
    df.printSchema()
    df.show()

    val workingDirPath = System.getProperty("user.dir")
    val modelBasePath = s"$workingDirPath/../examples/models/mnist/model"
    val tpoInfo = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.analysis.TensorflowPredictOperator")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri(modelBasePath)
          .setModelName("mnist")
          .setSignatureName("predict_images"))
      .build()
    val predictor = new TensorflowPredictOperator(tpoInfo)
    val result = predictor.operate(df)
    result.show()
    result.printSchema()
    result.toJSON.foreach(println(_))

//    val kwInfo = StreamWriterInfo.newBuilder()
//      .setId(1)
//      .setPrevId(0)
//      .setClsName("ksb.csle.component.writer.KafkaWriter")
//      .setKafkaWriter(KafkaInfo.newBuilder()
//          .setBootStrapServers("localhost:9092")
//          .setZooKeeperConnect("localhost:2181")
//          .setGroupId("mnist_output")
//          .setTopic("mnist_output"))
//       .build()
//    val writer = new KafkaWriter(kwInfo)
//    writer.write(result)
//    writer.close

    predictor.stop
  }

  private def testEnergyEecVectorInput(spark: SparkSession) {
    import spark.implicits._

    val seq = Seq(
        (Vectors.dense(Array(1.0)),
         Vectors.dense(Array.fill[Double](3360)(0)),
         Vectors.dense(Array.fill[Double](48)(0)),
         Vectors.dense(Array(1.0))))
    val df = seq.toDF("dp", "enc_inputs", "dec_inputs", "sp")
    df.printSchema()
    df.show()

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.data.integration.TensorflowPredictor")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri("/tmp/energy/eec/model")
          .setModelName("eec")
          .setSignatureName("pred24"))
      .build()
    val predictor = new TensorflowPredictOperator(info)
    val result = predictor.operate(df)
    result.show()
    result.printSchema()
    predictor.stop
  }

  private def testEnergyEecArrayInput(spark: SparkSession) {
    import spark.implicits._

    val seq = Seq(
        (Array(1f),
         Array.fill[Float](3360)(0).toSeq,
         Array.fill[Float](48)(0).toSeq,
         Array(1f)))
    val df = seq.toDF("dp", "enc_inputs", "dec_inputs", "sp")
    df.printSchema()
    df.show()

    val info = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.data.integration.TensorflowPredictor")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri("/tmp/energy/eec/model")
          .setModelName("eec")
          .setSignatureName("pred24"))
      .build()
    val predictor = new TensorflowPredictOperator(info)
    val result = predictor.operate(df)
    result.show()
    result.printSchema()
    predictor.stop
  }
}
