package ksb.csle.didentification.verification.loss

import scala.collection.mutable.Map

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.utilities._
import ksb.csle.didentification.interfaces._

/**
 * This class implements the loss measure method named as the cardinality
 * loss which measures the loss based on cardinality of a column.
 */
class CardinalityLoss extends InformationLoss {
  
  /**
   * Measures the information loss based on cardinality of a column.
   * 
   * @param src the original dataframe
   * @param anonymizedSrc the anonymized dataframe
   * @param suppressedSrc the suppressed dataframe
   * @param columnNames the array of column names of quasi-identifiers. The
   * combination key is made by cross-tabulating these variables.
   * @return InformationLossBound the measured information loss
   */
  override def lossMeasure(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      suppressedSrc: DataFrame,
      columnNames: Array[String]): InformationLossBound = {
    val anoLoss = getLossMeasure(src, anonymizedSrc, columnNames)
    val supLoss = getLossMeasure(src, suppressedSrc, columnNames)

    new InformationLossBound(supLoss, anoLoss, supLoss, anoLoss)
  }
  
  /**
   * Measures the information loss based on cardinality of a column.
   * 
   * @param src the original dataframe
   * @param anonymizedSrc the anonymized dataframe
   * @param columnNames the array of column names 
   * @return Double the measured information loss
   */
  private def getLossMeasure(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      columnNames: Array[String]): Double = {
    var mse = 0.0
    columnNames.map(column => {
      val originalUtil = getCardinality(src, column)
      val anonyUtil = getCardinality(anonymizedSrc, column)
      mse += {
        if(anonyUtil == originalUtil) 0.0
        else if(anonyUtil > originalUtil) 0.0 // impossible
        else (1.0 - anonyUtil / originalUtil)
      }
    })
    
    mse / columnNames.length
  }
  
  /**
   * Gets cardinality of 'column' column in 'src' dataframe
   * 
   * @param src the dataframe
   * @param column the column to get cardinality
   * @return Double the cardinality of the column
   */
  def getCardinality(
      src: DataFrame,
      column: String): Double = 
    src.select(column).dropDuplicates.count.toDouble
  
  /**
   * Gets cardinality of the array of columns in 'src' dataframe
   * 
   * @param src the dataframe
   * @param columns the array of columns to get cardinality
   * @return Double the cardinality of the column
   */
  def getCardinality(
      src: DataFrame,
      columnNames: Array[String]): Double = 
    src.select(columnNames.map(x => col(x)): _*).dropDuplicates.count.toDouble
        
}

object CardinalityLoss {
  def apply(): CardinalityLoss = new CardinalityLoss()
}
