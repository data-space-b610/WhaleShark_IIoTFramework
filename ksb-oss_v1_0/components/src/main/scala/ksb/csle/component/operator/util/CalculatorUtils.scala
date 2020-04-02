package ksb.csle.component.operator.util

import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import ksb.csle.component.exception._

object CalculatorUtils {

  @throws(classOf[KsbException])
  def getAverageListAllFromDF(df: DataFrame): Array[Double] = {
    try {
      val columnNames = df.columns
      var ret = new Array[Double](columnNames.length)

      for(i <- 0 to (columnNames.length - 1)) {
        val colName = columnNames(i)
        val average =
          df.selectExpr( s"cast($colName as double) $colName")
          .agg(avg(colName)).collect()(0).getDouble(0)
        ret(i) = average
      }
      ret
    }catch {
        case e: Exception => throw new DataException(s"casting Error : ", e)
    }
  }

  def round(input: Double, decimal: Int) = {
    val decimalNum = 10^decimal
    (input * decimalNum).round / decimalNum.toDouble
  }
}
