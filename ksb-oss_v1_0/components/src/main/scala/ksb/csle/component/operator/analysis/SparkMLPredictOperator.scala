package ksb.csle.component.operator.analysis

import scala.util.{ Try, Success, Failure }

import org.apache.spark.sql.DataFrame
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types._

import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.OndemandOperatorProto.SparkMLStreamPredictOperatorInfo
import ksb.csle.common.utils.resolver.PathResolver
import ksb.csle.component.operator.analysis.SparkMLPredictOperator.loadModel

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs predition with ML model in the given dataframe.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SparkMLStreamPredictOperatorInfo]]
 *          SparkMLStreamPredictOperatorInfo contains attributes as follows:
 *          - clsNameForModel: Class name for loading ml model.
 *                             "org.apache.spark.ml.PipelineModel" is highly recommended.(required)
 *          - modelPath: Path for ml model.
 *                       The path should contain metadata and stage directory
 *                       in the path (required).
 *
 *  ==SparkMLStreamPredictOperatorInfo==
 *  {{{
 *  message SparkMLStreamPredictOperatorInfo {
 *  required string clsNameForModel = 1;
 *  required string modelPath = 2;
 *  }
 *  }}}
 */
class SparkMLPredictOperator(
    o: StreamOperatorInfo) extends BaseGenericOperator[StreamOperatorInfo, DataFrame](o){

  val predictorInfo: SparkMLStreamPredictOperatorInfo = o.getMlStreamPredictor
  val model: org.apache.spark.ml.PipelineModel = loadModel(predictorInfo.getClsNameForModel, predictorInfo.getModelPath)

  def predict(df: DataFrame): DataFrame = {
    logger.info(s"OpId: ${o.getId}, ClassName: ${o.getClsName}")

    val ret =
    try {
      if(df.rdd.isEmpty) df
      else {
        val schema = new StructType().add("features", new VectorUDT())
        val schemaFieldNames = df.schema.fieldNames.toSeq
        var schemaFieldType =  df.schema(0).dataType
        logger.debug(df.show.toString)
        logger.debug(df.printSchema.toString)
        logger.debug("Data Type: " + schemaFieldType)
        df.select(schemaFieldNames.map(c => df.col(c).cast(schemaFieldType.typeName)): _*)
        val assembler = new VectorAssembler()
           .setInputCols(schemaFieldNames.toArray)
           .setOutputCol("features")
        logger.debug(df.show.toString)
        logger.debug(df.printSchema.toString)

        model.transform(assembler.transform(df)).drop("features")
      }
    } catch {
      case e: Exception =>
      model.transform(df)
    }
    logger.info("Run ML Prediction Ok!")
    logger.debug(ret.show().toString())
    ret
  }
  /**
   * execute prediction with SparkML Model.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = predict(df)
}

object SparkMLPredictOperator extends Logging {
  def apply(o: StreamOperatorInfo): SparkMLPredictOperator = new SparkMLPredictOperator(o)

  private[analysis] def loadModel(clsName: String, modelPath: String) = {
    logger.debug("loading model...")
    Try {
        org.apache.spark.ml.PipelineModel.load(modelPath)
    } match {
      case Success(model) =>
        logger.debug(s"model loaded from path ${modelPath}...")
        model
      case Failure(_) =>
        val model_Path = PathResolver.getPathByModelManager(modelPath, false)
        logger.info(model_Path)
        logger.debug(s"model loading from path ${model_Path}...")
        org.apache.spark.ml.PipelineModel.load(model_Path)
    }

  }
}
