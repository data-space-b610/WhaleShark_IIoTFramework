package ksb.csle.component.operator.cleaning

import scala.collection.JavaConversions._
import scala.util.Random

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.KMedoidsInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs KMedoids clustering. It partitions the data set of
 * n rows into k clusters. It chooses data points as centers and uses the
 * Manhattan Norm to calculate distance between data points.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.KMedoidsInfo]]
 *          KMedoidsInfo contains attributes as follows:
 *          - k_value: Number of clusters to form (required)
 *          - maxIter: Maximal number of iterations performed for one run of
 *                     KMedoids (required)
 *          - seed: Local random seed (optional)
 *
 *  ==KMedoidsInfo==
 *  {{{
 *  message KMedoidsInfo {
 *  required int32 k_value = 3 [default = 2];
 *  required int32 maxIter = 4 [default = 100];
 *  optional int64 seed = 5
 *  }
 *  }}}
 */
class KMedoidsOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with BaseDistanceCalculator {

  val p: ksb.csle.common.proto.StreamOperatorProto.KMedoidsInfo = o.getKMedoids

  /**
   * Validates KMedoids info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      val k: Int = p.getKValue
      if(!(1 < k && k <= df.count))
        throw new DataException("K value is invalidate")
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
   * Runs KMedoids operation using following params.
   */
  @throws(classOf[KsbException])
  private def kMedoids(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : kMedoids")
    try {
      validate(df)
      val maxIterations = p.getMaxIter
      val k = p.getKValue
      val seed = p.getSeed
      val r = new Random(seed)
      var matrix: Seq[Row] = df.collect.toSeq
      var medoids: Seq[Row] =
        r.shuffle(df.collect.distinct.toList).take(k).toSeq
      println(medoids.toList)
      var currentCost = modelCost(medoids, matrix)
      println(currentCost)
      var itr = 1
      var halt = false
      var closeMedoids = medoids
      while (!halt) {
        medoids = changeMedoids(itr, medoids, matrix)
        val nextCost = modelCost(medoids, matrix)
        if(currentCost > nextCost) {
          currentCost = nextCost
          closeMedoids = medoids
        }
        if(itr >= maxIterations)
          halt = true
        itr += 1
        val quot = itr / matrix.length
        if(k - 1 == quot) {
          r.setSeed(seed+itr)
          medoids = r.shuffle(df.collect.distinct.toList).take(k).toSeq
          println(medoids)
        }
      }
      println(closeMedoids)
      println(currentCost)
      val result = matrix.map(x => medoidIdx(x, closeMedoids))
      val w = Window.partitionBy(lit(1)).orderBy(lit(1))
      var cluster_df: DataFrame =
        df.sparkSession.createDataFrame(
            result.map(Tuple1.apply)).toDF("CLUSTER")
          .withColumn("INDEX", row_number.over(w))
      var df2: DataFrame = df.withColumn("INDEX", row_number.over(w))
      df2 = df2.join(cluster_df, "INDEX").sort(asc("INDEX")).drop("INDEX")
      df2
    } catch {
      case e: Exception => throw new ProcessException(
          s"KMedoids Process Error : ", e)
    }
  }

  /**
   * Calculates the Distance among medoids.
   */
  private def medoidDist(r: Row, medoids: Seq[Row]): Double = {
    val n = medoids.length
    var mMin = Double.MaxValue
    var j = 0
    while (j < n) {
      val m = ManhattanDistanceMetric(r, medoids(j))
      if (m < mMin) {
        mMin = m
      }
      j += 1
    }
    mMin
  }

  private def metric(x: Row, y: Row): Double = {
    val n = x.length
    var j = 0
    var s = 0.0
    while (j < n) {
      s += math.abs(x(j).toString().toDouble - y(j).toString().toDouble)
      j += 1
    }
    s
  }

  private def medoidIdx(r: Row, medoids: Seq[Row]): Int = {
    val n = medoids.length
    var mMin = Double.MaxValue
    var jMin = 0
    var j = 0
    while (j < n) {
      val m = ManhattanDistanceMetric(r, medoids(j))
      if (m < mMin) {
        mMin = m
        jMin = j
      }
      j += 1
    }
    jMin
  }

  private def modelCost(medoids: Seq[Row], data: Seq[Row]) =
    data.map(medoidDist(_, medoids)).sum

  private def changeMedoids(itr: Int, medoids: Seq[Row], data: Seq[Row]): Seq[Row] = {
    val n = data.length
    val remainder = itr % data.length
    var changeVal = medoids.updated(medoids.length - 1, data(remainder))
    changeVal
  }

  /**
   * Operates kMedoids.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = kMedoids(df)
}

object KMedoidsOperator {
  def apply(o: StreamOperatorInfo): KMedoidsOperator =
    new KMedoidsOperator(o)
}