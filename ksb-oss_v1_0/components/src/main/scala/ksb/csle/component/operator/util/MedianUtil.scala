package ksb.csle.component.operator.util

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * MedianUtil object to calculate the median value of a specific column in a dataframe.
 */
object MedianUtil {

  /**
   * Calculates a median value using following params.
   * 
   * @param dataset Input dataframe.
   * @param colNames The column names to be calculated.
   * @return Map[String, Any] The map of colNames and median values.
   */ 
  def median(
      dataset: Dataset[Row],
      colNames: Array[String]): Map[String, Any] = {
    val medians = colNames.map(median(dataset, _))
    colNames.zip(medians).toMap
  }

  private def median(dataset: Dataset[Row], colName: String): Any = {
    val dataType = dataset.schema(colName).dataType
    val median =
      if (dataType.isInstanceOf[NumericType]) {
        val sorted = dataset.select(colName).sort(colName)
          .rdd.zipWithIndex().map {
            case (v, idx) => (idx, v)
          }
        val count = sorted.count()
        import org.apache.spark.rdd.PairRDDFunctions
        val pair = new PairRDDFunctions(sorted)
        if (count % 2 == 0) {
          val left = count / 2 - 1
          val right = left + 1
          val lvalue = pair.lookup(left).head.get(0)
          val rvalue = pair.lookup(right).head.get(0)
          val sum = dataType match {
            case IntegerType => (lvalue.asInstanceOf[Int] + 
              rvalue.asInstanceOf[Int]).toDouble
            case DoubleType => (lvalue.asInstanceOf[Double] + 
              rvalue.asInstanceOf[Double])
          }
          (sum / 2)
        } else {
          val medianRow = pair.lookup(count / 2).head
          dataType match {
            case IntegerType => medianRow.getInt(0)
            case DoubleType => medianRow.getDouble(0)
          }
        }
      } else {
        Double.NaN
      }
    (median)
  }
}