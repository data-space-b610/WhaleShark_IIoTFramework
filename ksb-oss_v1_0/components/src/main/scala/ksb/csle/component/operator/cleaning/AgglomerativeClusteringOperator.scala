package ksb.csle.component.operator.cleaning

import scala.collection.JavaConversions._
import scala.util.Random
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Stack
import scala.util.control.Breaks._

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
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
import ksb.csle.common.proto.StreamOperatorProto.AgglomerativeClusteringInfo
import ksb.csle.common.proto.StreamOperatorProto.AgglomerativeClusteringInfo.LinkType
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.operator.util._
import ksb.csle.component.operator.common.BaseDistanceCalculator
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs Agglomerative clustering.
 * It aims to make a hierarchy of clusters in bottom-up approach.
 * Each observation begins with its own cluster and pairs of clusters are
 * combined as the hierarchy goes up.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AgglomerativeClusteringInfo]]
 *          AgglomerativeClusteringInfo contains attributes as follows:
 *          - numberOfClusters: Number of clusters to form (required)
 *          - link: Cluster mode. Enum(SINGLE, COMPLETE, AVERAGE), (required)
 *
 *  ==AgglomerativeClusteringInfo==
 *  {{{
 *  message AgglomerativeClusteringInfo {
 *  required int32 numberOfClusters = 3;
 *  required LinkType link = 4 [default = SINGLE];
 *  enum LinkType {
 *    SINGLE = 0;
 *    COMPLETE = 1;
 *    AVERAGE = 2;
 *  }
 *  }}}
 */
class AgglomerativeClusteringOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) with BaseDistanceCalculator {

  val p: ksb.csle.common.proto.StreamOperatorProto.AgglomerativeClusteringInfo =
    o.getAgglomerativeClustering

  /**
   * Validates AgglomerativeClustering info and dataframe schema info.
   */
  @throws(classOf[KsbException])
  private def validate(df: DataFrame) {
    try {
      val numberOfCluster: Int = p.getNumberOfClusters
      if(!(0 < numberOfCluster && numberOfCluster <= df.count))
        throw new DataException("numberOfCluster value is invalidate")
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
   * Runs AgglomerativeClustering operation using following params.
   */
  @throws(classOf[KsbException])
  private def agglomerativeClustering(
      df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : agglomerativeClustering")
    validate(df)
    var result: DataFrame = null
    try {
      var matrix = df.collect.map(row => {
        (row.toSeq.toArray.map({
          case l: Long => l
          case i: Int => i
          case d: Double => d
          case f: Float => f
          case _ => 0.0
        }))
      })
      val numberOfClusters: Int = p.getNumberOfClusters
      val linkage: LinkType = p.getLink
      val clusterMap = new HashMap[Int, HashSet[Int]]()
      val remaining = new HashSet[Int]()
      for (r <- 0 until matrix.size) {
        remaining.add(r)
        clusterMap(r) = HashSet(r)
      }
      while (clusterMap.size > numberOfClusters) {
        findColseDistance(clusterMap, remaining, matrix)
      }
      println(clusterMap.values.toList)
      val makeResult =
        udf[Int, Int](x => findCluster(x, clusterMap.values.toList))
      val w = Window.partitionBy(lit(1)).orderBy(lit(1))
      var df2: DataFrame = df.withColumn("INDEX", row_number.over(w) - 1)
      df2 = df2.withColumn("CLUSTER", makeResult(df2("INDEX"))).drop("INDEX")
      df2
    } catch {
      case e: Exception => throw new ProcessException(
          s"AgglomerativeClustering Process Error : ", e)
    }
  }

  /**
   * Finds a cluster in clusterMap.
   */
  private def findCluster(index: Int, clusterMap: List[HashSet[Int]]): Int = {
    var cluster: Int = -1
    breakable {
      for(i <- 0 until p.getNumberOfClusters) {
         if(clusterMap(i).contains(index)) {
           cluster = i
           break
         }
      }
    }
    cluster
  }

  private def findColseDistance(clusterMap: HashMap[Int, HashSet[Int]],
      remaining: HashSet[Int], matrix: Array[Array[Double]]): (Double, Int) = {
    var linkSim: Double = 0.0d
    var next: Int = 0
    var findValue: Int = 0
    var minValue = Double.MaxValue
    for (key <- clusterMap.keys) {
      val tmpValue = remaining.map(i =>
        if(key == i)
          (Double.MaxValue, i)
        else
          (getDistance(clusterMap(key), clusterMap(i), matrix), i)
          ).min
      if (minValue > tmpValue._1) {
        minValue = tmpValue._1
        linkSim = tmpValue._1
        next = tmpValue._2
        findValue = key
      }
    }
    remaining.remove(next)
    val c1Points = removeCluster(clusterMap, findValue)
    val c2Points = removeCluster(clusterMap, next)
    clusterMap(findValue) = c1Points ++ c2Points
    (linkSim, next)
  }

  /**
   * Removes the clusterMap of corresponding Id.
   */
  private def removeCluster(clusterMap: HashMap[Int, HashSet[Int]],
      id: Int): HashSet[Int] = {
    val s = clusterMap.remove(id).get
    s
  }

  /**
   * Calculates the linkage.
   */
  private def getDistance(x: HashSet[Int], y: HashSet[Int],
      matrix: Array[Array[Double]]): Double = {
    val n: Double = x.size * y.size
    var data: Double = 0.0d
    if(p.getLink == LinkType.SINGLE) {
      data = Double.MaxValue
    }
    else if(p.getLink == LinkType.COMPLETE) {
      data = Double.MinValue
    }
    var sum: Double = 0.0d
    for(c1 <- x) {
      for(c2 <- y) {
        val tmp = metric(matrix(c1), matrix(c2))
        if(p.getLink == LinkType.SINGLE) {
          if(data > tmp) {
             data = tmp
           }
        }
        else if(p.getLink == LinkType.COMPLETE) {
           if(data < tmp) {
             data = tmp
           }
        }
        else if(p.getLink == LinkType.AVERAGE) {
           sum += tmp
        }
      }
    }
    if(p.getLink == LinkType.AVERAGE)
      data = sum / n
    data
  }

  /**
   * Calculates the cost of distance.
   */
  private def metric(x: Array[Double], y: Array[Double]): Double = {
    val n = x.length
    var j = 0
    var s = 0.0
    while (j < n) {
      s += math.abs(x(j).toString().toDouble - y(j).toString().toDouble)
      j += 1
    }
    if(s == 0.0)
      s = Double.MaxValue
    s
  }

  /**
   * Operates AgglomerativeClustering.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */

  def operate(df: DataFrame): DataFrame = agglomerativeClustering(df)
}

object AgglomerativeClusteringOperator {
  def apply(o: StreamOperatorInfo): AgglomerativeClusteringOperator =
    new AgglomerativeClusteringOperator(o)
}