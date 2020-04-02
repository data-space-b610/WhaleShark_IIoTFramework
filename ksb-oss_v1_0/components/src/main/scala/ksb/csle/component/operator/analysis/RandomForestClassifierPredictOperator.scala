package ksb.csle.component.operator.analysis

import java.io.{File, FileFilter}

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs the prediction according to a given RandomForest classification model.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.RandomForestClassifierPredictInfo]]
 *          RandomForestClassifierPredictInfo contains attributes as follows:
 *          - modelBasePath: model base directory path (required)
 *          - modelId: model identifier to load; if not set, load the latest model (optional)
 *
 * ==RandomForestClassifierPredictInfo==
 * {{{
 * message RandomForestClassifierPredictInfo {
 *   required string modelBasePath = 1;
 *   optional string modelId = 2;
 * }
 * }}}
 */
class RandomForestClassifierPredictOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getRandomForestClassifierPredictInfo == null) {
      throw new IllegalArgumentException(
          "RandomForestClassifierPredictInfo is not set.")
    } else {
      o.getRandomForestClassifierPredictInfo
    }

  private[this] val modelPath =
    (info.getModelBasePath, info.getModelId) match {
      case (null | "", _) =>
        throw new IllegalArgumentException("modelBasePath is not set.")
      case (basePath, null | "") =>
        basePath + "/" + latestModelId(basePath)
      case (basePath, modelId) =>
        basePath + "/" + modelId
  }

  private[this] val pipelineModel =
    Try {
      PipelineModel.load(modelPath)
    } match {
      case Success(model) =>
        logger.info(s"RandomForestClassifier: load model from '$modelPath'")
        model
      case Failure(e) =>
        logger.error(
            s"RandomForestClassifier: can't load model from '$modelPath'",
            e)
        throw e
    }

  /**
   * Operates the RandomForest classification model prediction.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has prediction output columns.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    Try {
      val predictions = pipelineModel.transform(inputs)
      toPretty(predictions)
    } match {
      case Success(result) =>
        logger.info("RandomForestClassifier: prediction finished")
        result
      case Failure(e) =>
        logger.error("RandomForestClassifier: prediction failed", e)
        inputs
    }
  }

  private def latestModelId(modelBasePath: String): String = {
    val baseDir = new File(modelBasePath)
    if (baseDir.exists() == false) {
      throw new IllegalArgumentException(
          "modelBasePath not exist: " + modelBasePath)
    }

    val subDirs = baseDir.listFiles(new FileFilter {
      override def accept(f: File): Boolean = {
        if (f.isFile()) {
          false
        } else {
          Try(f.getName().toDouble) match {
            case Success(_) => true
            case Failure(_) => false
          }
        }
      }})

    val latestId = subDirs.length match {
      case 1 =>
        subDirs.head.getName
      case _ =>
        subDirs.map(_.getName).max
    }
    latestId
  }

  private def toPretty(result: DataFrame): DataFrame = {
    val vecToArray = udf { (v: Vector) => v.toArray }
    val pretty1 = result
      .withColumn("probability" , vecToArray(col("probability")))

    val labelIndexer = pipelineModel.stages(1).asInstanceOf[StringIndexerModel]
    val probSplitExpr = labelIndexer.labels.zipWithIndex.map {
      case (name, index) =>
        val newColName = "probability(" + name + ")"
        (newColName, col("probability").getItem(index))
    }
    val pretty2 = probSplitExpr.foldLeft(pretty1) {
      case (src, (name, expr)) =>
        src.withColumn(name, expr)
    }

    pretty2
      .drop("features")
      .drop(labelIndexer.getOutputCol)
      .drop("rawPrediction")
      .drop("prediction")
      .drop("probability")
  }
}
