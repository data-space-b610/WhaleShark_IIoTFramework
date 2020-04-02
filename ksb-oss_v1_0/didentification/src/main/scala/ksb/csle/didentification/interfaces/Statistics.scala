package ksb.csle.didentification.interfaces

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This trait provides the statistics related functions such as
 * min, max, avg, std, and # of records. 
 */
trait Statistics extends DataFrameCheck {
  
  /**
   * Gets the minimum value among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return String the string type of Minimum value
   */
  def getMinValue(src: DataFrame, columnName: String): String = {
    require(isValidColumnName(src, columnName), s"no columns named as $columnName") 
    src.select(min(columnName)).first.get(0).toString
  }

  /**
   * Gets the minimum value among the given list of values
   * 
   * @param values the list of values
   * @return Double Minimum value
   */
  def getMinValue(values: List[Double]): Double = {
    require(values.length > 0, "the list should have one or more entries")
    values.reduceLeft(_ min _)
  }
    
  /**
   * Gets the maximum value among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return String the string type of Maximum value
   */
  def getMaxValue(src: DataFrame, columnName: String): String = { 
    require(isValidColumnName(src, columnName), s"no columns named as $columnName") 
    src.select(max(columnName)).first.get(0).toString
  }
      
  /**
   * Gets the maximum value among the given list of values
   * 
   * @param src Dataframe
   * @param values the list of values
   * @return Double Maximum value
   */
  def getMaxValue(values: List[Double]): Double = {
    require(values.length > 0, "the list should have one or more entries")
    values.reduceLeft(_ max _)
  }
    
  /**
   * Gets the average value among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return String The string type of average value
   */
  def getAvgValue(src: DataFrame, columnName: String): String = {
    require(isValidColumnName(src, columnName), s"no columns named as $columnName") 
    src.select(avg(columnName)).first.get(0).toString
  }

  /**
   * Gets the average value among the given list of values
   * 
   * @param src Dataframe
   * @param values the list of values
   * @return Double The average value
   */
  @throws(classOf[java.lang.ArithmeticException])
  def getAvgValue(values: List[Long]): Double = { 
    require(values.length > 0, "the list should have one or more entries")
    values.sum / values.length
  }

  /**
   * Gets the standard deviation value among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return String The string type of standard deviation value
   */
  def getStdValue(src: DataFrame, columnName: String): String = {
    require(isValidColumnName(src, columnName), s"no columns named as $columnName") 
    src.select(stddev(columnName)).first.get(0).toString
  }

  /**
   * Gets the average value among the given list of values
   * 
   * @param src Dataframe
   * @param values the list of values
   * @return Double The average value
   */
  def getStdValue(values: List[Long]): Double = {
    require(values.length > 0, "the list should have one or more entries")
    val avg = values.sum.toDouble / values.size
    values.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / values.size
  }

  /**
   * Gets the number of tuples among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return Double The number of tuples
   */
  def getCountValue(src: DataFrame, columnName: String): String = {
    require(isValidColumnName(src, columnName), s"no columns named as $columnName") 
    src.count.toString
  }

  /**
   * Gets the number of tuples among he given list of values
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return Double The number of tuples
   */
  def getCountValue(values: List[Long]): Double = { 
    require(values.length > 0, "the list should have one or more entries")
    values.length.toDouble
  }
    
}