package ksb.csle.component.operator.cleaning

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer
import scala.util.control.Breaks._

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.{LinearRegression => MlLinearRegression}
import org.apache.spark.ml.linalg.Vectors

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.LinearRegressionInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs Linear Regression. It aims to model the relationship
 * between two variables by fitting a linear equation to observed data.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.LinearRegressionInfo]]
 *          LinearRegressionInfo contains attributes as follows:
 *          - labelName: Column name of label. (required)
 *          - regParam: Regularization parameter (required)
 *          - elasticNetParam: ElasticNet mixing parameter (required)
 *                             For alpha = 0, the penalty is an L2 penalty.
 *                             For alpha = 1, it is an L1 penalty.
 *                             For alpha in (0,1), the penalty is a combination
 *                             of L1 and L2.
 *          - maxIter: Maximum number of iterations (required)
 *          - tolerance: minimum tolerance to eliminate collinear attributes
 *                       (required)
 *
 *  ==LinearRegressionInfo==
 *  {{{
 *  message LinearRegressionInfo {
 *  required string labelName = 3;
 *  required double regParam = 4;
 *  required double elasticNetParam = 5;
 *  required int32 maxIter = 6;
 *  required double tolerance = 7;
 *  }
 *  }}}
 */
class LinearRegressionOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with BaseDistanceCalculator {

  val p: ksb.csle.common.proto.StreamOperatorProto.LinearRegressionInfo =
    o.getLinearRegression

  /**
   * Validates LinearRegression info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
      if(!df.columns.contains(p.getLabelName))
        throw new DataException("Label column is not exist")
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  /**
   * Runs LinearRegression operation using following params.
   */
  @throws(classOf[KsbException])
  private def linearRegression(df: DataFrame): DataFrame = {
    validate(df)
    var result: DataFrame = null
    try {
      var label_df: DataFrame = df.select(p.getLabelName)
      val vectors = df.drop(p.getLabelName).rdd.map(row => {
        Vectors.dense(row.toSeq.toArray.map({
          case l: Long => l
          case i: Int => i
          case d: Double => d
          case f: Float => f
        }))
      })
      var features_df: DataFrame =
        df.sparkSession.createDataFrame(
            vectors.map(Tuple1.apply)).toDF("features")
      val w = Window.partitionBy(lit(1)).orderBy(lit(1))
      label_df = label_df.withColumn("INDEX", row_number.over(w))
      features_df = features_df.withColumn("INDEX", row_number.over(w))
      label_df =
        label_df.join(features_df, "INDEX").sort(asc("INDEX")).drop("INDEX")
      val lir = new MlLinearRegression()
        .setFeaturesCol("features")
        .setLabelCol(p.getLabelName)
        .setRegParam(p.getRegParam)
        .setElasticNetParam(p.getElasticNetParam)
        .setMaxIter(p.getMaxIter)
        .setTol(p.getTolerance)
      val lirModel = lir.fit(label_df)
      result = df.sparkSession.createDataFrame(
          df.drop(p.getLabelName).columns.map(Tuple1.apply))
        .toDF("attribute")
        .withColumn("INDEX", row_number.over(w))
      var weight_result: DataFrame = result.sparkSession.createDataFrame(
          lirModel.coefficients.toArray.map(Tuple1.apply))
        .toDF("weight")
        .withColumn("INDEX", row_number.over(w))
      result = result.join(weight_result, "INDEX").sort(asc("INDEX")).drop("INDEX")
      println(s"Weights: ${lirModel.coefficients}" +
        s" Intercept: ${lirModel.intercept}")
    } catch {
      case e: Exception => throw new ProcessException(
          s"LinearRegression Process Error : ", e)
    }
    result
  }

  /**
   * Operates LinearRegression.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = linearRegression(df)
}

object LinearRegressionOperator {
  def apply(o: StreamOperatorInfo): LinearRegressionOperator =
    new LinearRegressionOperator(o)
}