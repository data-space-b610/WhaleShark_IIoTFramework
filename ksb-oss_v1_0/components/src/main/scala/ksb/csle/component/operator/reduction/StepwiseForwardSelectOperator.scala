package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.StepwiseForwardSelectionInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.ingestion.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs 'forward selection' which is the deterministic greedy
 * feature selection algorithm. It selects the most relevant columns.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.StepwiseForwardSelectionInfo]]
 *          StepwiseForwardSelectionInfo contains attributes as follows:
 *          - labelName: Column name of label (required)
 *          - pValue: P-value of estimated coefficients and intercept (required)
 *
 *  ==StepwiseForwardSelectionInfo==
 *  {{{
 *  message StepwiseForwardSelectionInfo {
 *  required string labelName = 3;
 *  required double pValue = 4 [default = 0.01];
 *  }
 *  }}}
 */
class StepwiseForwardSelectOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.StepwiseForwardSelectionInfo =
    o.getStepwiseForwardSelection

  /**
   * validates StepwiseForwardSelection info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {      if(!(p.getPValue >= 0d && p.getPValue <= 1d))
        throw new DataException("P-Value range over.")
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
      if(!df.columns.contains(p.getLabelName))
        throw new DataException("Label column is not exist")
    } catch {
      case e: DataException => throw e
      case e: Exception     => throw new DataException(s"validate Error : ", e)
    }
  }

  /**
   * Run stepwiseForwardSelection operation using following params.
   */
  @throws(classOf[KsbException])
  private def stepwiseForwardSelection(df: DataFrame): DataFrame = {
    validate(df)
    logger.info("P-Value : " + p.getPValue)
    try {
      var remainAttributes: Array[String] = df.drop(p.getLabelName).columns
      var selectResult: Array[String] = new Array[String](0)
      var (bestIndex, bestString) = getFirstBestAttribute(remainAttributes, df)
      if(bestIndex != -1) {
        remainAttributes = remove(remainAttributes, bestIndex)
        selectResult :+= bestString
      }
      while(bestIndex != -1 && remainAttributes.size > 0) {
        var maxTValue: Double = Double.MinValue
        bestIndex = -1
        bestString = ""
        for(i <- 0 until remainAttributes.size) {
          val lirModel = getLirModel(selectResult :+ remainAttributes(i), df)
          val summary = lirModel.summary
          if (summary.pValues(selectResult.size) <= 0.01) {
            if (maxTValue < math.abs(summary.tValues(selectResult.size))) {
              maxTValue = math.abs(summary.tValues(selectResult.size))
              bestIndex = i
              bestString = remainAttributes(i)
            }
          }
        }
        if(bestIndex != -1) {
          remainAttributes = remove(remainAttributes, bestIndex)
          selectResult :+= bestString
        }
      }
      val result: DataFrame = df.select(p.getLabelName, selectResult:_*)
      result
    } catch {
      case e: Exception => throw new ProcessException(
          s"StepwiseForwardSelection Process Error : ", e)
    }
  }

  private def remove(a: Array[String], i: Int): Array[String] = {
    val b = a.toBuffer
    b.remove(i)
    b.toArray
  }

  private def getFirstBestAttribute(
      features: Array[String], df: DataFrame): (Int, String) = {
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")
    val output = assembler.transform(df).select(p.getLabelName,"features")
    val lir = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol(p.getLabelName)
    val lirModel = lir.fit(output)
    val summary = lirModel.summary
    var maxTValue: Double = Double.MinValue
    var bestIndex: Int = -1
    var bestString: String = ""
    for(i <- 0 until features.size) {
      if(summary.pValues(i) <= p.getPValue) {
        if(maxTValue < math.abs(summary.tValues(i))) {
          maxTValue = math.abs(summary.tValues(i))
          bestIndex = i
          bestString = features(i)
        }
      }
    }
    (bestIndex, bestString)
  }

  private def getLirModel(
      features: Array[String], df: DataFrame): LinearRegressionModel = {
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")
    val output = assembler.transform(df).select(p.getLabelName,"features")
    val lir = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol(p.getLabelName)
    val lirModel = lir.fit(output)
    lirModel
  }

  /**
   * Operates Aggregate.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = stepwiseForwardSelection(df)
}

object StepwiseForwardSelectOperator {
  def apply(o: StreamOperatorInfo): StepwiseForwardSelectOperator =
    new StepwiseForwardSelectOperator(o)
}