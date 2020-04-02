package ksb.csle.component.operator.common

import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}

trait BaseDistanceCalculator {
   def ManhattanDistanceMetric(x: Row, y: Row): Double = {
    val n = x.length
    var j = 0
    var s = 0.0
    while (j < n) {
      s += math.abs(x(j).toString().toDouble - y(j).toString().toDouble)
      j += 1
    }
    s
  }
}