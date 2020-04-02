package ksb.csle.didentification.utilities

import scala.collection.mutable.Map

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.didentification.interfaces._

/**
 * This object provides some functions to manage outliers.
 */
object OutlierManager extends Statistics with MethodString {
  
  private val boxplotCoeff = 0.5
  private val zscoreCoeff = 1.5
  
  /**
   * Same as makeAgeStatTable(src: DataFrame, columnName: String, 
   * method: String), but the method is the type of AggregationMethod.
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Boxplot] Outlier management table (
   *  			 the Boxplot information in a interval)
   */
  def makeAgeOutlierMgmtTableBoxplot[T](
      src: DataFrame, 
      columnName: String, 
      method: T): Map[Interval, OutlierInfo] = 
    makeAgeOutlierMgmtTableBoxplot(src, columnName, getMethodString(method))
  
  /**
   * In case of age-related column, the outlier information info may be
   * decided by the 10s, 20s, and so on. 
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Boxplot] Outlier management table (
   *  			 the Boxplot information in a interval)
   */
  def makeAgeOutlierMgmtTableBoxplot(
      src: DataFrame, 
      columnName: String,
      method: String): Map[Interval, OutlierInfo] = {
    val table = Map[Interval, OutlierInfo]()
    for (i <- 0 to (getMaxValue(src, columnName).toDouble / 10).toInt) {
      val subset = src.filter(src.col(columnName) >= i * 10 && 
          src.col(columnName) < (i+1) * 10)
      if(subset.count != 0) {
        val quantiles = subset.stat.approxQuantile(columnName, Array(0.25, 0.75), 0.0)
        val IQR = quantiles(1) - quantiles(0)
        val replaceValue = StatisticManager.
          getRepresentiveValue(subset, columnName, method)

        table += (new Interval(i*10, (i+1)*10) -> 
          new OutlierInfo(quantiles(0) - boxplotCoeff * IQR, 
              quantiles(1) + boxplotCoeff * IQR, replaceValue.toString))
      }
    }
    
    (table)
  }

  /**
   * Same as makeOutlierMgmtTableBoxplot(src: DataFrame, columnName: String, 
   * method: String), but the method is the type of AggregationMethod
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Boxplot] Outlier management table (
   *  			 the Boxplot information in a interval)
   */
  def makeOutlierMgmtTableBoxplot[T](
      src: DataFrame, 
      columnName: String,
      method: T): Map[Interval, OutlierInfo] =
    makeOutlierMgmtTableBoxplot(src, columnName, getMethodString(method))
 
  /**
   * Makes the outlier management table based on the boxplot technique which
   * includes outlier information about some numerical interval as a form
   * of map [numerical interval, outlier management info]. The default
   * interval is set to satisfy the number of intervals to be be 10 
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Boxplot] Outlier management table (
   *  			 the Boxplot information in a interval)
   */
  def makeOutlierMgmtTableBoxplot(
      src: DataFrame, 
      columnName: String,
      method: String): Map[Interval, OutlierInfo] = {
    makeOutlierMgmtTableBoxplot(src, columnName, method, 10)
  }
        
  def makeOutlierMgmtTableBoxplot[T](
      src: DataFrame, 
      columnName: String,
      method: T,
      nSteps: Int): Map[Interval, OutlierInfo] =
    makeOutlierMgmtTableBoxplot(src, columnName, getMethodString(method), nSteps)
 
  /**
   * Makes the outlier management table based on the boxplot technique which
   * includes outlier information about some numerical interval as a form
   * of map [numerical interval, outlier management info]. 
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param interval interval
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Boxplot] Outlier management table (
   *  			 the Boxplot information in a interval)
   * 
   */
  def makeOutlierMgmtTableBoxplot(
      src: DataFrame, 
      columnName: String,
      method: String,
      nSteps: Int): Map[Interval, OutlierInfo] = {
    val maxValue = getMaxValue(src, columnName).toDouble
    val minValue = getMinValue(src, columnName).toDouble
    val step = (maxValue - minValue) / nSteps
      
    val table = Map[Interval, OutlierInfo]()
    for (i <- 0 until nSteps) {
      val subset = src.filter(src.col(columnName) >= i * step + minValue && 
          src.col(columnName) < (i+1) * step + minValue)
      if(subset.count != 0) {
        val quantiles = subset.stat.approxQuantile(columnName, Array(0.25, 0.75), 0.0)
        val IQR = quantiles(1) - quantiles(0)
        val replaceValue = StatisticManager.
          getRepresentiveValue(subset, columnName, method)

        table += (new Interval(i*step+minValue, (i+1)*step+minValue) -> 
          new OutlierInfo(quantiles(0) - boxplotCoeff * IQR, 
              quantiles(1) + boxplotCoeff * IQR, replaceValue.toString))
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
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Boxplot] Outlier management table (
   *  			 the Boxplot information in a interval)
   */
  def makeAgeOutlierMgmtTableZscore[T](
      src: DataFrame, 
      columnName: String, 
      method: T): Map[Interval, OutlierInfo] =
    makeAgeOutlierMgmtTableZscore(src, columnName, getMethodString(method))
  
  /**
   * In case of age-related column, the outlier information info may be
   * decided by the 10s, 20s, and so on. 
   * 
   * @param src Dataframe
   * @param columnName Column 
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Zscore] Outlier management table (
   *  			 the Z-score information in a interval)
   */
  def makeAgeOutlierMgmtTableZscore(
      src: DataFrame, 
      columnName: String, 
      method: String): Map[Interval, OutlierInfo] = {
    val table = Map[Interval, OutlierInfo]()
    for (i <- 0 to (getMaxValue(src, columnName).toDouble / 10).toInt) {
      val subset = src.filter((src.col(columnName) >= i * 10 && 
          src.col(columnName) < (i+1) * 10) )
      if(subset.count != 0) {
        val lowerBound = getAvgValue(subset, columnName).toDouble -
            zscoreCoeff * getStdValue(subset, columnName).toDouble
        val upperBound = getAvgValue(subset, columnName).toDouble +
            zscoreCoeff * getStdValue(subset, columnName).toDouble
        val replaceValue = StatisticManager.
          getRepresentiveValue(subset, columnName, method)

        table += (new Interval(i*10, (i+1)*10) -> 
          new OutlierInfo(lowerBound, upperBound, replaceValue.toString))      
      }
    }
    
    (table)    
  }

  /**
   * Same as makeOutlierMgmtTableZscore(src: DataFrame, columnName: String, 
   * method: String), but the method is the type of AggregationMethod
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count 
   * @return Map[Interval, Zscore] Outlier management table (
   *  			 the Z-score information in a interval)
   */
  def makeOutlierMgmtTableZscore[T](
      src: DataFrame, 
      columnName: String,
      method: T): Map[Interval, OutlierInfo] = 
    makeOutlierMgmtTableZscore(src, columnName, getMethodString(method))
  
  /**
   * Makes the outlier management table based on the z-score technique which
   * includes outlier information about some numerical interval as a form
   * of map [numerical interval, outlier management info]. The default
   * interval is set to satisfy the number of intervals to be be 10 
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Zscore] Outlier management table (
   *  			 the Z-score information in a interval)
   */
  def makeOutlierMgmtTableZscore(
      src: DataFrame, 
      columnName: String,
      method: String): Map[Interval, OutlierInfo] = {
    makeOutlierMgmtTableZscore(src, columnName, method, 10)
  }
  
  def makeOutlierMgmtTableZscore[T](
      src: DataFrame, 
      columnName: String,
      method: T,
      nSteps: Int): Map[Interval, OutlierInfo] = 
    makeOutlierMgmtTableZscore(src, columnName, getMethodString(method), nSteps)
  
  /**
   * Makes the outlier management table based on the z-score technique which
   * includes outlier information about some numerical interval as a form
   * of map [numerical interval, outlier management info]. 
   * 
   * @param src Dataframe
   * @param columnName Column
   * @param interval interval
   * @param method Methods to handle outliers. ex., min, max, avg, std, and count
   * @return Map[Interval, Zscore] Outlier management table (
   *  			 the Z-score information in a interval)
   */
  def makeOutlierMgmtTableZscore(
      src: DataFrame, 
      columnName: String,
      method: String,
      nSteps: Int): Map[Interval, OutlierInfo] = {
    val maxValue = getMaxValue(src, columnName).toDouble
    val minValue = getMinValue(src, columnName).toDouble
    val step = (maxValue - minValue) / nSteps
    
//    println("min" + minValue + "max" + maxValue + "step" + step)
    val table = Map[Interval, OutlierInfo]()
    for (i <- 0 until nSteps) {
      val subset = src.filter(src.col(columnName) >= i*step+minValue && 
          src.col(columnName) < (i+1)*step+minValue)
      if(subset.count != 0) {
        val lowerBound = getAvgValue(subset, columnName).toDouble -
            zscoreCoeff * getStdValue(subset, columnName).toDouble
        val upperBound = getAvgValue(subset, columnName).toDouble +
            zscoreCoeff * getStdValue(subset, columnName).toDouble
        val replaceValue = StatisticManager.
          getRepresentiveValue(subset, columnName, method)

        table += (new Interval(i*step+minValue, (i+1)*step+minValue) -> 
          new OutlierInfo(lowerBound, upperBound, replaceValue.toString))
        
      }
    }
    
    (table)
  }

}