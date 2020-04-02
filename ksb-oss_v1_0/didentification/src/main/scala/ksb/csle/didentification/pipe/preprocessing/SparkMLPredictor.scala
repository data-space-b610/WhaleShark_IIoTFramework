package ksb.csle.didentification.pipe.preprocessing

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.PipelineModel

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

class SparkMLPredictor(
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  val p: EnergyMLStreamPredictPipeOperatorInfo = o.getEnergyMLPredict
  val model: org.apache.spark.ml.PipelineModel = loadModel(p.getClsNameForModel)

  import s.implicits._

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

  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = predict(df)

}
