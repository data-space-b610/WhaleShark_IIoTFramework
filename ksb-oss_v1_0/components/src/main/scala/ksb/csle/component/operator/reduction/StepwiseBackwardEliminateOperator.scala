package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.Window

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamOperatorProto.StepwiseBackwardEliminationInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.ingestion.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs 'backward elimination' which is the deterministic
 * greedy feature selection algorithm. It selects the most relevant columns.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.StepwiseBackwardEliminationInfo]]
 *          StepwiseBackwardEliminationInfo contains attributes as follows:
 *          - labelName: Column name of label (required)
 *          - pValue: P-value of estimated coefficients and intercept (required)
 *
 *  ==StepwiseBackwardEliminationInfo==
 *  {{{
 *  message StepwiseBackwardEliminationInfo {
 *  required string labelName = 3;
 *  required double pValue = 4 [default = 0.01];
 *  }
 *  }}}
 */
class StepwiseBackwardEliminateOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.StepwiseBackwardEliminationInfo =
    o.getStepwiseBackwardElimination

  /**
   * validates StepwiseBackwardElimination info and dataframe schema info.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {
      if(!(p.getPValue >= 0d && p.getPValue <= 1d))
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

  @throws(classOf[KsbException])
  private def stepwiseBackwardElimination(df: DataFrame): DataFrame = {
    validate(df)
    logger.info("P-Value : " + p.getPValue)
    try {
      var remainAttributes: Array[String] = df.drop(p.getLabelName).columns
      var worstIndex: Int = 0
      var bestString: String = ""
      while(worstIndex != -1 && remainAttributes.size > 0) {
        worstIndex = getWorstAttribute(remainAttributes, df)
        if (worstIndex != -1) {
          remainAttributes = remove(remainAttributes, worstIndex)
        }
      }
      val result: DataFrame = df.select(p.getLabelName, remainAttributes:_*)
      result
    } catch {
      case e: Exception => throw new ProcessException(
          s"stepwiseBackwardElimination Process Error : ", e)
    }
  }

  private def remove(a: Array[String], i: Int): Array[String] = {
    val b = a.toBuffer
    b.remove(i)
    b.toArray
  }

  private def getWorstAttribute(
      features: Array[String], df: DataFrame): Int = {
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")
    val output = assembler.transform(df).select(p.getLabelName,"features")
    val lir = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol(p.getLabelName)
    val lirModel = lir.fit(output)
    val summary = lirModel.summary
    var minTValue: Double = Double.MaxValue
    var worstIndex: Int = -1
    for(i <- 0 until features.size) {
      if(summary.pValues(i) > p.getPValue) {
        if(minTValue > math.abs(summary.tValues(i))) {
          minTValue = math.abs(summary.tValues(i))
          worstIndex = i
        }
      }
    }
    worstIndex
  }

  /**
   * Operates Aggregate.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = stepwiseBackwardElimination(df)
}

object StepwiseBackwardEliminateOperator {
  def apply(o: StreamOperatorInfo): StepwiseBackwardEliminateOperator =
    new StepwiseBackwardEliminateOperator(o)
}