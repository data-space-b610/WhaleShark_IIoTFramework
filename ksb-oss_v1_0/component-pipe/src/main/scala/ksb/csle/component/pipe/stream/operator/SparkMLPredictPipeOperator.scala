package ksb.csle.component.pipe.stream.operator

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.spark.sql.functions._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.PipelineModel

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

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
class SparkMLPredictPipeOperator(
    o: StreamPipeOperatorInfo,
    session: SparkSession
    ) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, session) {

  val p: EnergyMLStreamPredictPipeOperatorInfo = o.getEnergyMLPredict
  val model: org.apache.spark.ml.PipelineModel = loadModel(p.getClsNameForModel)

  import session.implicits._

  def loadModel(modelPath: String): PipelineModel = {
    logger.info("Loading MODEL PATH INFO: " + modelPath)
    val model = org.apache.spark.ml.PipelineModel.load(modelPath)
    logger.debug("loading finished...")
    (model)
  }

  def predict(df: DataFrame => DataFrame): DataFrame => DataFrame =
    df => {
      logger.info(s"OpId: ${o.getId}, ClassName: ${o.getClsName}")
      val assembler = new VectorAssembler()
          .setInputCols(df.columns)
          .setOutputCol("features")

      model.transform(assembler.transform(df))
    }

  /**
   * Execute prediction with ML model.
   *
   * @param  df Input function pipe
   * @return output data pipe
   */
  override def operate(
      df: DataFrame => DataFrame): DataFrame => DataFrame = predict(df)

}
