package ksb.csle.didentification.utilities

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamDidentProto._

/**
 * This object provides some functions to generalize the dataframe.
 * In order to get generalized dataframe, should specify the column
 * of dataframe to apply the generalization function, and its possible
 * maximum generalization step and desired generalization step. 
 */
object GeneralizeManager {
  
  /**
   * Gets the dataframe which is made by the generalization function. 
   * The default function generalizes the first column of given dataframe
   * by the first generalized step among the maximum five generalized steps. 
   * 
   * @param src Dataframe 
   * @param columnName The column name 
   * @param numLevels The maximum generalization step 
   * @param currLevels The step to want to generalize the dataframe 
   * @return DataFrame The generalized dataframe
   */
  def generalizing(src: DataFrame): DataFrame = 
    generalizing(src, src.columns(0), 5, 1)
    
  def generalizing(src: DataFrame, numLevels: Int): DataFrame =
    generalizing(src, src.columns(0), numLevels, 1)
    
  def generalizing(src: DataFrame, numLevels: Int, curLevel: Int): DataFrame =
    generalizing(src, src.columns(0), numLevels, curLevel)
    
  def generalizing(src: DataFrame, columnName: String): DataFrame = 
    generalizing(src, columnName, 5, 1)
    
  def generalizing(src: DataFrame, columnName: String, numLevels: Int): DataFrame =
    generalizing(src, columnName, numLevels, 1)
        
  def generalizing(
      src: DataFrame,
      columnName: String,
      numLevels: Int,
      currLevel: Int): DataFrame = {
    val result = src.schema(columnName).dataType match {
      case n: NumericType => 
        getNumGeneralizing(src, columnName, numLevels, currLevel)
      case s: StringType => 
        getStrGeneralizing(src, columnName, numLevels, currLevel)
      case _ => 
        getStrGeneralizing(src, columnName, numLevels, currLevel)
    }
    
    result
  }
  
  /**
   * Gets the dataframe made by the generalization function by using the
   * given 'genColInfo' information. The 'genColInfo' info includes
   * the information about the column to generalize, maximum generalization
   * step, and desired generalization step.
   * 
   * @param src Dataframe 
   * @param genColInfo The information about the column to generalize
   * @return DataFrame The generalized dataframe
   */
  def generalizing(
      src: DataFrame,
      genColInfo: GeneralizeColumnInfo): DataFrame = {
    val columnName = src.columns(genColInfo.getSelectedColumnId)
    val numLevels = genColInfo.getNumLevels
    val currLevel = genColInfo.getCurLevel
    generalizing(src, columnName, numLevels, currLevel)
  }

  /**
   * Gets the dataframe made by the generalization function. This function
   * is applied on the string type of columns. 
   * 
   * @param src Dataframe 
   * @param columnName The column name (
   * @param numLevels The maximum generalization step 
   * @param currLevels The step to want to generalize the dataframe 
   * @return DataFrame The generalized dataframe
   */
  def getStrGeneralizing(
      src: DataFrame,
      columnName: String,
      numLevels: Int,
      currLevel: Int): DataFrame = {
    def generalize: (String => String) = value => 
      if(currLevel != 0) 
        value.dropRight((value.length.toDouble / numLevels * currLevel).toInt).concat("*")
      else value
    
    val generalizeUdf = udf(generalize)
    src.withColumn(columnName, generalizeUdf(src.col(columnName)))
  }
  
  /**
   * Gets the dataframe made by the generalization function. This function
   * is applied on the numerical type of columns. 
   * 
   * @param src Dataframe 
   * @param columnName The column name (
   * @param numLevels The maximum generalization step 
   * @param currLevels The step to want to generalize the dataframe 
   * @return DataFrame The generalized dataframe
   */
  def getNumGeneralizing(
      src: DataFrame,
      columnName: String,
      numLevels: Int,
      currLevel: Int): DataFrame = {
    import scala.math.pow
    
    val maxValue = src.agg(max(src.col(columnName))).first.get(0).toString.toDouble
    val step = maxValue / pow(2, numLevels)
      
    def generalize: (String => String) = value =>
      if(currLevel != 0) {
        val position = (value.toDouble / (step * pow(2, currLevel))).toInt
        (position * step * pow(2, currLevel)) + "~" + (position + 1) * step * pow(2, currLevel)
      }
      else value
    
    val generalizeUdf = udf(generalize)
    src.withColumn(columnName, generalizeUdf(src.col(columnName)))
  }
  
  /**
   * Returns the hierarchy information of the 'columnName' column of 'src'
   * dataframe. The returned dataframe is composed of the following form:
   * [column, 1-st generalized column, 2-nd generalized column, ..., n-th
   * generalized column]. 
   * 
   * @param src Dataframe 
   * @param columnName The column name (
   * @param numLevels The maximum generalization step 
   * @return DataFrame Hierarhcy information of 'columnName' column of 'src' dataframe
   */
  def makeGeneralizationHierarchy(
      src: DataFrame,
      columnName: String,
      numLevels: Int): DataFrame = {
    val result = src.schema(columnName).dataType match {
      case n: NumericType =>
        makeNumericGeneralizationHierarchy(src, columnName, numLevels)
      case s: StringType =>
        makeStringGeneralizationHierarchy(src, columnName, numLevels)
      case _ => makeStringGeneralizationHierarchy(src, columnName, numLevels)
    }
    
    result
  }
  
  /**
   * Returns the hierarchy information of the 'columnName' column, which is
   * the string type of column, of 'src' dataframe. 
   * 
   * @param src Dataframe 
   * @param columnName The column name (
   * @param numLevels The maximum generalization step 
   * @return DataFrame Hierarhcy information of 'columnName' column of 'src' dataframe
   */
  def makeStringGeneralizationHierarchy(
      src: DataFrame,
      columnName: String,
      numLevels: Int): DataFrame = {
    var result = src.select(columnName).dropDuplicates
    
    for(i <- 1 to numLevels) {
      def generalize: (String => String) = value => 
        value.dropRight((value.length.toDouble / numLevels * i).toInt).concat("*")
      
      val generalizeUdf = udf(generalize)
      result = result.withColumn(columnName.concat(i.toString),
          generalizeUdf(result.col(columnName)))
    }
    
    (result)
  }
 
  /**
   * Returns the hierarchy information of the 'columnName' column, which is
   * the numerical type of column, of 'src' dataframe. 
   * 
   * @param src Dataframe 
   * @param columnName The column name (
   * @param numLevels The maximum generalization step 
   * @return DataFrame Hierarhcy information of 'columnName' column of 'src' dataframe
   */
  def makeNumericGeneralizationHierarchy(
      src: DataFrame,
      columnName: String,
      numLevels: Int): DataFrame = {
    import scala.math.pow
    
    var result = src.select(columnName).sort(columnName).dropDuplicates
    val maxValue = src.agg(max(src.col(columnName))).first.get(0).toString.toDouble
    val step = maxValue / pow(2, numLevels)
    for(i <- 1 to numLevels) {
      def generalize: (String => String) = value => {
        val position = (value.toDouble / (step * pow(2, i))).toInt
        (position * step * pow(2, i)) + "~" + (position + 1) * step * pow(2, i) 
      }
      
      val generalizeUdf = udf(generalize)
      result = result.withColumn(columnName.concat(i.toString),
          generalizeUdf(result.col(columnName)))
    }
    
    (result)
  }
  
}