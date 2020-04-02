package ksb.csle.component.runner

import org.apache.logging.log4j.scala.Logging
import org.scalatest._
import org.scalatest.Assertions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{ HashingTF, Tokenizer }
import org.apache.spark.sql.SparkSession
import ksb.csle.common.base.UnitSpec
import ksb.csle.common.utils.SparkUtils
import scala.reflect.runtime.universe

class SparkMLPredictRunnerTest extends UnitSpec with Logging {

  "loadModel" should "load model from model path" in {

    val clsName = "org.apache.spark.ml.PipelineModel"
    val modelPath = "/tmp/spark-logistic-regression-model"
    val (dataset, myModel) = getDummyModel(modelPath)

    // FIXME: compile error.
    //    val yourModel = SparkMLPredictRunner.loadModel(clsName, modelPath)
    //    yourModel.transform(dataset)
    //
    //    assert(myModel.transform(dataset) === yourModel.transform(dataset))
  }

  def getDummyModel(modelPath: String) = {
    val spark: SparkSession = SparkUtils.getSparkSession("appName", "local[*]")

    // Prepare training documents from a list of (id, text, label) tuples.
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0))).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))
    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)
    // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save(modelPath)
    (training, model)
  }
}
