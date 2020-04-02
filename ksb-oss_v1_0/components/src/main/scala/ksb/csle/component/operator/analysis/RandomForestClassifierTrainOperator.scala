package ksb.csle.component.operator.analysis

import java.text.SimpleDateFormat
import java.util.Date
import java.io.{DataOutputStream, FileOutputStream}
import java.net.URI

import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that trains RandomForest classification model in the given attributes.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.RandomForestClassifierTrainInfo]]
 *          RandomForestClassifierTrainInfo contains attributes as follows:
 *          - labelColumnName: label column name (required)
 *          - featureColumnNames: feature column names (required)
 *          - modelBasePath: model base path to save (required)
 *          - numFolds: number of folds for cross validation, default is 3 (optional)
 *          - numTrees: number of trees to train (>= 1), default is 20 (repeated)
 *          - maxDepth: maximum depth of the tree (>= 0), default is 5 (repeated)
 *          - maxBins: maximum number of bins used for discretizing continuous features, default is 32 (repeated)
 *          - featureSubsetStrategy: number of features to consider for splits at each tree node, default is 'auto' (repeated)
 *          - impurity: criterion used for information gain calculation, default is 'gini' (repeated)
 *
 * ==RandomForestClassifierTrainInfo==
 * {{{
 * message RandomForestClassifierTrainInfo {
 *   enum FeatureSubsetStrategy {
 *     auto = 1;
 *     all = 2;
 *     onethird = 3;
 *     sqrt = 4;
 *     log2 = 5;
 *   }
 *
 *   enum Impurity {
 *     gini = 1;
 *     entropy = 2;
 *   }
 *
 *   required string labelColumnName = 1;
 *   required string featureColumnNames = 2 [default = "*"];
 *   required string modelBasePath = 3;
 *   optional int32 numFolds = 4 [default = 3];
 *   repeated int32 numTrees = 5; // [default = 20];
 *   repeated int32 maxDepth = 6; // [default = 5];
 *   repeated int32 maxBins = 7; // [default = 32];
 *   repeated FeatureSubsetStrategy featureSubsetStrategy = 8; // [default = auto];
 *   repeated Impurity impurity = 9; // [default = gini];
 * }
 * }}}
 */
class RandomForestClassifierTrainOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val HISTORY_HEADER = "modelId,fmeasure,accuracy,preceision,recall,numFolds,numTrees,maxDepth,maxBins,featureSubsetStrategy,impurity"

  private[this] val info =
    if (o.getRandomForestClassifierTrainInfo == null) {
      throw new IllegalArgumentException(
          "RandomForestClassifierTrainInfo is not set.")
    } else {
      o.getRandomForestClassifierTrainInfo
    }

  private[this] val indexedLabelColumnName = info.getLabelColumnName match {
    case null | "" =>
      throw new IllegalArgumentException("labelColumnName is not set.")
    case _ =>
      s"indexed${info.getLabelColumnName}"
  }

  private[this] val modelId =
    new SimpleDateFormat("yyMMdd.HHmmss").format(new Date())

  private[this] val modelSavePath = info.getModelBasePath match {
    case null | "" =>
      throw new IllegalArgumentException("modelBasePath is not set.")
    case basePath: String =>
      basePath + "/" + modelId
  }

  /**
   * Operate training the RandomForest classification model.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has test results.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    Try {
      val Array(trainingData, testData) = inputs.randomSplit(Array(0.8, 0.2))

      logger.info("RandomForestClassifier: preparing pipeline and params ...")
      val pipeline = buildPipeline(trainingData)
      val paramMaps = buildParamMaps(pipeline)

      logger.info("RandomForestClassifier: training ...")
      val cvModel = train(trainingData, pipeline, paramMaps)

      logger.info("RandomForestClassifier: testing ...")
      val testResult = test(testData, cvModel)
      toPretty(testResult, pipeline)
    } match {
      case Success(result) =>
        logger.info("RandomForestClassifier: trained successfully")
        result
      case Failure(e) =>
        logger.error("RandomForestClassifier: training is failed", e)
        throw e
    }
  }

  private def buildPipeline(inputs: DataFrame): Pipeline = {
    import inputs.sparkSession.implicits._

    val featureColumnNames = info.getFeatureColumnNames match {
      case null | "" =>
        throw new IllegalArgumentException("featureColumnNames is not set.")
      case "*" =>
        inputs.schema.fieldNames.filterNot(
            _.equalsIgnoreCase(info.getLabelColumnName))
      case _ =>
        info.getFeatureColumnNames.split(",").map(_.trim())
    }
    val featureAssembler = new VectorAssembler()
      .setInputCols(featureColumnNames)
      .setOutputCol("features")

    val labelIndexer = new StringIndexer()
      .setInputCol(info.getLabelColumnName)
      .setOutputCol(indexedLabelColumnName)
      .fit(inputs)

    val classifier = new RandomForestClassifier()
      .setLabelCol(indexedLabelColumnName)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol(s"prediction(${info.getLabelColumnName})")
      .setLabels(labelIndexer.labels)

    new Pipeline().setStages(
        Array(featureAssembler, labelIndexer, classifier, labelConverter))
  }

  private def buildParamMaps(pipeline: Pipeline): Array[ParamMap] = {
    val classifier = pipeline.getStages(2)
      .asInstanceOf[RandomForestClassifier]
    val builder = new ParamGridBuilder()

    // default = 20
    if (info.getNumTreesCount > 0) {
      val values = info.getNumTreesList.map(_.toInt)
        .filter(_ > 0)
      builder.addGrid(classifier.numTrees, values)
    }

    // default = 5, currently(spark 2.3.0) only supports maxDepth <= 30
    if (info.getMaxDepthCount > 0) {
      val values = info.getMaxDepthList.map(_.toInt)
        .filter { v => v > 0 && v <= 30 }
      builder.addGrid(classifier.maxDepth, values)
    }

    // default = 32, Must be >= 2
    if (info.getMaxBinsCount > 0) {
      val values = info.getMaxBinsList.map(_.toInt)
        .filter(_ >= 2)
      builder.addGrid(classifier.maxBins, values)
    }

    // default = "auto"
    if (info.getFeatureSubsetStrategyCount > 0) {
      val values = info.getFeatureSubsetStrategyList.map(_.name())
      builder.addGrid(classifier.featureSubsetStrategy, values)
    }

    // default = "gini"
    if (info.getImpurityCount > 0) {
      val values = info.getImpurityList.map(_.name())
      builder.addGrid(classifier.impurity, values)
    }

    builder.build()
  }

  private def train(dataset: DataFrame, pipeline: Pipeline,
      paramMaps: Array[ParamMap]): CrossValidatorModel = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(indexedLabelColumnName)
      .setPredictionCol("prediction")
      .setMetricName("f1")

    // default = 3, Must be >= 2
    val numFolds =
      if (info.getNumFolds < 2) 3
      else info.getNumFolds

    val validator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
      .setNumFolds(numFolds)

    validator.fit(dataset)
  }

  private def test(dataset: DataFrame,
      cvModel: CrossValidatorModel): DataFrame = {
    val bestPipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    bestPipelineModel.write.save(modelSavePath)

    val predictions = bestPipelineModel.transform(dataset)
    val predictionAndLabels = predictions.select (
        col("prediction"),
        col(indexedLabelColumnName).cast(types.DoubleType)
      ).rdd.map { case Row(prediction: Double, label: Double) =>
        (prediction, label)
      }
    val metrics = new MulticlassMetrics(predictionAndLabels)

    val rfcModel = bestPipelineModel.stages(2)
      .asInstanceOf[RandomForestClassificationModel]

    val historyLine = Array(modelId, metrics.weightedFMeasure,
        metrics.accuracy, metrics.weightedPrecision, metrics.weightedRecall,
        cvModel.getNumFolds, rfcModel.getNumTrees, rfcModel.getMaxDepth,
        rfcModel.getMaxBins, rfcModel.getFeatureSubsetStrategy,
        rfcModel.getImpurity).mkString(",")
    val historyFilePath = new Path(info.getModelBasePath + "/history.csv")
    val conf = new Configuration()
    conf.set("dfs.replication", "1")
    val fs = FileSystem.get(URI.create(info.getModelBasePath), conf,
        System.getProperty("user.name"))
    val historyOutStream =
      if (fs.exists(historyFilePath)) {
        fs match {
          case _: DistributedFileSystem =>
            fs.append(historyFilePath)
          case _: LocalFileSystem =>
            new DataOutputStream(
                new FileOutputStream(historyFilePath.toString(), true))
        }
      } else {
        val out = fs.create(historyFilePath)
        out.writeChars(HISTORY_HEADER)
        out.writeChar('\n')
        out
      }
    historyOutStream.writeChars(historyLine)
    historyOutStream.writeChar('\n')
    historyOutStream.close()
    fs.close()

    predictions
  }

  private def toPretty(testResult: DataFrame,
      pipeline: Pipeline): DataFrame = {
    val vecToArray = udf { (v: Vector) => v.toArray }
    val pretty1 = testResult
      .withColumn("probability" , vecToArray(col("probability")))

    val labelIndexer = pipeline.getStages(1).asInstanceOf[StringIndexerModel]
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
      .drop(indexedLabelColumnName)
      .drop("rawPrediction")
      .drop("prediction")
      .drop("probability")
  }
}
