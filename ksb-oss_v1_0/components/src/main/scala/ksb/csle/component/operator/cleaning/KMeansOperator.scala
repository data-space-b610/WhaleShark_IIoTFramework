package ksb.csle.component.operator.cleaning

import scala.util.control.Breaks._
import scala.collection.JavaConversions._

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.mllib.clustering.KMeans.{K_MEANS_PARALLEL, RANDOM}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

import ksb.csle.component.exception._
import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.KMeansInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs K-Means clustering.
 * It partitions n rows into k clusters in which each row belongs to the
 * cluster with the nearest mean.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.KMeansInfo]]
 *          KMeansInfo contains attributes as follows:
 *          - k_value: Number of clusters to form (required)
 *          - maxIterations: Maximal number of iterations performed for one
 *                     run of k-Means (required)
 *          - maxRuns: This param has no effect in spark 2.3.0
 *          - seed: Local random seed (optional)
 *
 *  ==KMeansInfo==
 *  {{{
 *  message KMeansInfo {
 *  required int32 k_value = 3 [default = 2];
 *  required int32 maxIterations = 4 [default = 100]
 *  required int32 maxRuns = 5 [default = 10];
 *  optional int64 seed = 6;
 *  }
 *  }}}
 */
class KMeansOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.KMeansInfo = o.getKMeans

  /**
   * Validates kMeans info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      for (field <- df.schema.fields) {
        if(!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
    } catch {
      case e: DataException => throw e
      case e: Exception => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  /**
   * Runs K_Means operation using following params.
   */
  @throws(classOf[KsbException])
  private def kMeans(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : K_Means")
    val k: Int = p.getKValue
    val maxIterations: Int = p.getMaxIterations
    val maxRuns: Int = p.getMaxRuns
    val seed: Long = p.getSeed
    var result = validate(df)
    import df.sqlContext.implicits._
    try {
      val vectors = df.rdd.map(row => {
        Vectors.dense(row.toSeq.toArray.map({
          case l: Long => l
          case i: Int => i
          case d: Double => d
          case _ => 0.0
        }))
      })
      vectors.cache()
      var kMeansModel: KMeansModel = null
      if(seed == 0) {
        kMeansModel = KMeans.train(vectors, k, maxIterations, maxRuns, K_MEANS_PARALLEL)
      }
      else {
        kMeansModel = KMeans.train(vectors, k, maxIterations, maxRuns, K_MEANS_PARALLEL, seed)
      }
      kMeansModel.clusterCenters.foreach(println)
      val predictions = kMeansModel.predict(vectors)
      import df.sqlContext.implicits._
      val w = Window.partitionBy(lit(1)).orderBy(lit(1))
      val predDF = predictions.toDF("CLUSTER").withColumn("INDEX", row_number.over(w))
      result = result.withColumn("INDEX", row_number.over(w))
      result = result.join(predDF, "INDEX").sort(asc("INDEX")).drop("INDEX")
    } catch {
      case e: Exception => throw new ProcessException(s"KMeans Process Error : ", e)
    }
    result
  }

  /**
   * Operates KMeans.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = kMeans(df)
}

object KMeansOperator {
  def apply(o: StreamOperatorInfo): KMeansOperator =
    new KMeansOperator(o)
}