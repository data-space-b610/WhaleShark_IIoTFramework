package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import scala.reflect.runtime.universe

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.didentification.exceptions._
import ksb.csle.didentification.interfaces._

/**
 * This object provides some functions to manage statistic information.
 */
object StatisticManager extends Statistics with MethodString {

  /**
   * Same as makeNumericStatTable(src: DataFrame, columnName: String, 
   * method: String), but the method is the type of AggregationMethod
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method AggregationMethod. ex., min, max, avg, and median 
   * @return Map[Interval, Double] Statistic table
   */
  def makeNumericStatTable[T](
      src: DataFrame, 
      columnName: String, 
      method: T): Map[Interval, String] = 
    makeNumericStatTable(src, columnName, getMethodString(method))

  /**
   * Makes the statistic table which includes statistic information
   * about some numerical interval as a form of map 
   * [numerical interval, statistics info].
   * The default number of intervals in a column is 10.
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Statistic methods. ex., min, max, avg, and median 
   * @return Map[Interval, Double] Statistic table
   */
  def makeNumericStatTable(
      src: DataFrame, 
      columnName: String, 
      method: String): Map[Interval, String] = 
    makeNumericStatTable(src, columnName, method, 10)
  
  /**
   * Same as makeNumericStatTable(src: DataFrame, columnName: String, 
   * method: String, nSteps: Int), but the method is the type of
   * AggregationMethod.
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method AggregationMethod. ex., min, max, avg, and median 
   * @param nSteps The number of intervals in a column
   * @return Map[Interval, Double] Statistic table
   */
  def makeNumericStatTable[T](
      src: DataFrame, 
      columnName: String, 
      method: T,
      nSteps: Int): Map[Interval, String] = 
    makeNumericStatTable(src, columnName, getMethodString(method), nSteps)
      
  /**
   * Makes the statistic table which includes statistic information
   * about some numerical interval as a form of map 
   * [numerical interval, statistics info].
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Statistic methods. ex., min, max, avg, and median 
   * @param nSteps The number of intervals in a column
   * @return Map[Interval, Double] Statistic table
   */
  def makeNumericStatTable(
      src: DataFrame, 
      columnName: String, 
      method: String,
      nSteps: Int): Map[Interval, String] = {
    val maxValue = getMaxValue(src, columnName).toDouble
    val minValue = getMinValue(src, columnName).toDouble
    val step = { 
      if(maxValue == minValue) 1
      else (maxValue - minValue) / nSteps
    }
    
    val table = Map[Interval, String]()
    for (i <- 0 to nSteps) {
      val subset = src.filter((src.col(columnName) >= minValue + i*step && 
          src.col(columnName) < minValue + (i+1)*step) )
      if(subset.count != 0) {
        val group = getRepresentiveValue(subset, columnName, method)
        table += (new Interval(minValue+i*step, minValue+(i+1)*step) -> group)
      }
    }
    
    (table)
  }
    
  /**
   * In case of age-related column, the statistic info may be decided
   * by the 10s, 20s, and so on. 
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Statistic methods. ex., min, max, avg, and median 
   * @return Map[Interval, Double] Statistic table
   */
  def makeAgeStatTable(
      src: DataFrame, 
      columnName: String, 
      method: String): Map[Interval, String] = {
    val table = Map[Interval, String]()
    for (i <- 0 to (getMaxValue(src, columnName).toDouble / 10).toInt) {
      val subset = src.filter((src.col(columnName) >= i * 10 && 
          src.col(columnName) < (i+1) * 10) )
      if(subset.count != 0) {
        val group = getRepresentiveValue(subset, columnName, method)
        table += (new Interval(i*10, (i+1)*10) -> group)
      }
    }
    
    (table)
  }

  /**
   * Same as makeAgeStatTable(src: DataFrame, columnName: String, 
   * method: String), but the method is the type of AggregationMethod.
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Statistic methods. ex., min, max, avg, and median 
   * @return Map[Interval, Double] Statistic table
   */
  def makeAgeStatTable[T](
      src: DataFrame, 
      columnName: String, 
      method: T): Map[Interval, String] =
    makeAgeStatTable(src, columnName, getMethodString(method))
  
  /**
   * Get the representative value of the 'columnName' column in 'src' data
   * frame.
   * 
   * @param src Dataframe
   * @param columnName Column name
   * @param method Statistic methods. ex., min, max, and avg 
   * @return Double The representative value
   */  
  def getRepresentiveValue(
      src: DataFrame, 
      columnName: String,
      method: String): String = 
        method match {
          case "MIN" => getMinValue(src, columnName).toString
          case "MAX" => getMaxValue(src, columnName).toString
          case "AVG" => getAvgValue(src, columnName).toString
          case "STD" => getStdValue(src, columnName).toString
          case "COUNT" => getCountValue(src, columnName).toString
          case "STAR" => "*"
          case "BLANK" => " "
          case "UNDERBAR" => "_"
          case _ => getAvgValue(src, columnName).toString
        }
  
  /**
   * Get the representative value of the 'columnName' column in 'src' data
   * frame.
   * 
   * @param src Dataframe
   * @param columnName Column name
   * @param method Statistic methods. ex., min, max, and avg 
   * @return Double The representative value
   */  
  def getRepresentiveValue[T](
      src: DataFrame, 
      columnName: String,
      method: T): String = 
    getRepresentiveValue(src, columnName, getMethodString(method))
  
  /**
   * Some columns may contain both numerical and non-numerical
   * values simultaneously, i.e., 21K, \$45, etc. In this case, this
   * function calculates the statistics information only considering
   * numerical parts, and then only replaces numerical parts with
   * calculated statistic info while sustaining the other parts. It 
   * makes the statistic table as a form of map 
   * [numerical interval, statistics info].
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Statistic methods. ex., min, max, avg, and median 
   * @return Map[Interval, Double] Statistic table
   */
  def makeMixedStatTable[T](
      src: DataFrame,
      columnName: String,
      method: String): Map[Interval, String] = {
    val reg = "([^0-9]+)([0-9]+)([^0-9]+)".r
    def extractNumeric: (String => String) = {
      key => key match {  
        case reg(_, b, _) => b
      }
    }
    
    val extractNumericUdf = udf(extractNumeric)
    val result = src.withColumn("tmpNumeric", extractNumericUdf(src.col(columnName)))
    makeNumericStatTable(result, "tmpNumeric", method)
  }
  
  /**
   * Same as makeMixedStatTable(src: DataFrame, columnName: String, 
   * method: String), but the method is the type of AggregationMethod
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method AggregationMethod. ex., min, max, avg, and median 
   * @return Map[Interval, Double] Statistic table
   */
  def makeMixedStatTable[T](
      src: DataFrame, 
      columnName: String, 
      method: T): Map[Interval, String] = 
    makeMixedStatTable(src, columnName, getMethodString(method))
  
  /**
   * Statistic table is composed of [Interval, statistic].
   * This function returns Interval which include given 'value'
   * 
   * @param table Statistic table
   * @param value Value to search
   * return Interval  
   */
  def getKey[A, B](
      table: Map[Interval, A],
      value: B): Interval = {
    import scala.util.control.Breaks._

    var interval = table.last._1
    breakable {
      table.map(element =>
        if(value.toString.toDouble >= element._1.lower 
            && value.toString.toDouble < element._1.upper) { 
              interval = element._1
              break
        }
      )
    }
    
    (interval)
  }
  
}