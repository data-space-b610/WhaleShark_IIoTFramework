package ksb.csle.component.operator.cleaning

import scala.collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Stack
import scala.util.control.Breaks._
import scala.util.Random

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.monotonicallyIncreasingId

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.EMClusteringInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs EM(Expectation–Maximization) Clustering.
 * It performs two steps iteratively to partition into k clusters.
 * Expectation step calculates the expected value of the log likelihood
 * function and maximization step finds the parameter maximizing the expected 
 * log-likelihood.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.EMClusteringInfo]]
 *          EMClusteringInfo contains attributes as follows:
 *          - k_value: Number of clusters to form (required)
 *          - maxIter: Maximal number of iterations to be performed for one 
 *                     run (required)
 *
 *  ==EMClusteringInfo==
 *  {{{
 *  message EMClusteringInfo {
 *  required int32 k_value = 3 [default = 2];
 *  required int32 maxIter = 4;
 *  }
 *  }}}
 */
class EMClusteringOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with BaseDistanceCalculator {

  val p: ksb.csle.common.proto.StreamOperatorProto.EMClusteringInfo =
    o.getEMClustering

  /**
   * Validates EMClustering info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {
      val k: Int = p.getKValue
      if(!(1 < k && k <= df.count))
        throw new DataException("K value is invalidate")
      if(!(0 < p.getMaxIter))
        throw new DataException("MaxIter value is invalidate")
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
    } catch {
      case e: DataException => throw e
      case e: Exception    => throw new DataException(s"validate Error : ", e)
    }
  }

  /**
   * Runs EMClustering operation using following params.
   */
  @throws(classOf[KsbException])
  private def eMClustering(df: DataFrame): DataFrame = {
    validate(df)
    val dfTemp = df.withColumn("INDEX",monotonicallyIncreasingId)
    try {
      val vectors = df.rdd.map(row => {
        Vectors.dense(row.toSeq.toArray.map({
          case l: Long => l
          case i: Int => i
          case d: Double => d
          case f: Float => f
        }))
      })
      val df2 =
        df.sparkSession.createDataFrame(vectors.map(Tuple1.apply))
         .toDF("features")
      val gmm = new GaussianMixture()
        .setK(p.getKValue)
        .setFeaturesCol("features")
        .setPredictionCol("cluster")
        .setProbabilityCol("probability")
        .setMaxIter(p.getMaxIter)
      val model = gmm.fit(df2)
      var result: DataFrame =
        model.transform(df2).drop("features", "probability")
      val resultTemp = result.withColumn("INDEX",monotonicallyIncreasingId)
      result = dfTemp.join(resultTemp,Seq("INDEX"),joinType="outer").sort(asc("INDEX")).drop("INDEX")
      for (i <- 0 until model.getK) {
        println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
          s"mu=${model.gaussians(i).mean}\nsigma=\n" +
          s"${model.gaussians(i).cov}\n")
      }
      result
    } catch {
      case e: Exception => throw new ProcessException(
          s"EMClustering Process Error : ", e)
    }
  }

  /**
   * Operates EMClustering.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = eMClustering(df)
}

object EMClusteringOperator {
  def apply(o: StreamOperatorInfo): EMClusteringOperator =
    new EMClusteringOperator(o)
}