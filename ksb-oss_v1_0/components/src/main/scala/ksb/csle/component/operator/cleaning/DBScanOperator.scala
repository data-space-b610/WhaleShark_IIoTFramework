package ksb.csle.component.operator.cleaning

import scala.collection.JavaConversions._
import scala.util.Random
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Stack
import scala.util.control.Breaks._

import com.google.protobuf.Message
import org.apache.spark.sql.{ Row, SparkSession, DataFrame, SQLContext }
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.DBSCANInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs DBSCAN(Density-based spatial clustering of
 * applications with noise).
 * It groups points with nearby neighobors and makes as outlies points that are
 * in low density region.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.DBSCANInfo]]
 *          DBSCANInfo contains attributes as follows:
 *          - eps: Epsilon which specifies the size of the neighborhood (required)
 *          - minPts: Minimal number of points to form a cluster (required)
 *
 *  ==DBSCANInfo==
 *  {{{
 *  message DBSCANInfo {
 *    required double eps = 3;
 *    required int32 minPts = 4;
 *  }
 *  }}}
 */
class DBScanOperator(
  o: StreamOperatorInfo
  ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with BaseDistanceCalculator {

  val p: ksb.csle.common.proto.StreamOperatorProto.DBSCANInfo = o.getDBSCAN

  /**
   * Validates DBSCAN info and dataframe schema info using following params.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame) {
    try {
      if (p.getEps < 0.0d)
        throw new DataException(s"eps value is invalidate : $p.getEps")
      if (p.getMinPts < 0)
        throw new DataException(s"minPts value is invalidate : $p.getMinPts")
      for (field <- df.schema.fields) {
        if (!field.dataType.isInstanceOf[NumericType])
          throw new DataException("DataFrame Schema Types are not NumericType")
      }
    } catch {
      case e: DataException => throw e
      case e: Exception     => throw new DataException(s"validate Error : ", e)
    }
  }

  /**
   * Runs DBSCAN operation using following params.
   */
  @throws(classOf[KsbException])
  private def dBSCAN(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : DBSCAN")
    validate(df)
    try {
      var matrix = df.collect.map(row => {
        (row.toSeq.toArray.map({
          case l: Long   => l.toDouble
          case i: Int    => i.toDouble
          case d: Double => d
          case f: Float  => f
          case _         => 0.0
        }))
      })
      val minPts: Double = p.getMinPts
      var cluster: Int = 0
      var visitList: Array[Array[Double]] = new Array[Array[Double]](0)
      var noiseList: Array[Array[Double]] = new Array[Array[Double]](0)
      val clusterMap = new HashMap[Int, HashSet[Array[Double]]]()
      for (matrix_data <- matrix) {
        if (!visitList.contains(matrix_data)) {
          visitList :+= matrix_data
          var neigh = getNeighbours(matrix_data, matrix)
          if (neigh.size >= minPts) {
            clusterMap(cluster) = HashSet(matrix_data)
            var index: Int = 0
            while (neigh.size > index) {
              val neigh_data = neigh(index)
              if (!visitList.contains(neigh_data)) {
                visitList :+= neigh_data
                val neigh2 = getNeighbours(neigh_data, matrix)
                if (neigh2.size >= minPts) {
                  neigh = mergeNeigh(neigh, neigh2)
                }
                if (!isExistData(neigh_data, clusterMap)) {
                  clusterMap(cluster) =
                    clusterMap.remove(cluster).get ++ HashSet(neigh_data)
                }
              }
              index += 1
            }
            cluster += 1
          }
        }
      }
      val w = Window.partitionBy(lit(1)).orderBy(lit(1))
      val result: Seq[Int] =
        matrix.map(x => findCluster(x, clusterMap.values.toList)).toSeq
      var cluster_df: DataFrame =
        df.sparkSession.createDataFrame(
          result.map(Tuple1.apply)).toDF("CLUSTER")
          .withColumn("INDEX", row_number.over(w))
      var df2: DataFrame = df.withColumn("INDEX", row_number.over(w))
      df2 = df2.join(cluster_df, "INDEX").sort(asc("INDEX")).drop("INDEX")
      df2
    } catch {
      case e: Exception => throw new ProcessException(
        s"DBSCAN Process Error : ", e)
    }
  }

  /**
   * Finds a cluster in clusterMap.
   */
  private def findCluster(
    index:      Array[Double],
    clusterMap: List[HashSet[Array[Double]]]): Int = {
    var cluster: Int = 0
    breakable {
      for (i <- 0 until clusterMap.size) {
        if (clusterMap(i).contains(index)) {
          cluster = i + 1
          break
        }
      }
    }
    cluster
  }

  /**
   * Merges two neighbors.
   */
  private def mergeNeigh(
    neigh:  Array[Array[Double]],
    neigh2: Array[Array[Double]]): Array[Array[Double]] = {
    var merge = neigh
    for (i <- 0 until neigh2.size) {
      if (!merge.contains(neigh2(i))) {
        merge :+= neigh2(i)
      }
    }
    merge
  }

  /**
   * Checks whether the data exists in clusterMap.
   */
  private def isExistData(
    data:       Array[Double],
    clusterMap: HashMap[Int, HashSet[Array[Double]]]): Boolean = {
    var find = false
    breakable {
      for (i <- 0 until clusterMap.size) {
        if (clusterMap(i).contains(data)) {
          find = true
          break
        }
      }
    }
    find
  }

  /**
   * Calculates the cost of distance
   */
  private def getDistance(x: Array[Double], y: Array[Double]): Double = {
    //manhattan measure
    val n = x.length
    var j = 0
    var s = 0.0
    while (j < n) {
      s += math.abs(x(j).toString().toDouble - y(j).toString().toDouble)
      j += 1
    }
    s
  }

  /**
   * Gets the list of neighbors.
   */
  private def getNeighbours(
    x:      Array[Double],
    matrix: Array[Array[Double]]): Array[Array[Double]] = {
    var neigh: Array[Array[Double]] = new Array[Array[Double]](0)
    val n = matrix.length
    for (i <- 0 until n) {
      val y: Array[Double] = matrix(i)
      if (getDistance(x, y) <= p.getEps) {
        neigh :+= y
      }
    }
    neigh
  }

  /**
   * Operates DBSCAN.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(
    df: DataFrame): DataFrame = dBSCAN(df)
}

object DBScanOperator {
  def apply(o: StreamOperatorInfo): DBScanOperator =
    new DBScanOperator(o)
}
