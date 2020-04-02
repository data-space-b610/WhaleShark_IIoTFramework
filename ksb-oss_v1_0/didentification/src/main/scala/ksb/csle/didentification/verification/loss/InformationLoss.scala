package ksb.csle.didentification.verification.loss

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.didentification.utilities._
import ksb.csle.didentification.interfaces._

/**
 * This abstract class defines some functions for measuring information loss.
 * To Be Modified for future
 */
abstract class InformationLoss extends Serializable {
  
  /**
   * Measures the information loss of anonymized data compared to the original data.
   * 
   * @param src the dataframe
   * @param anonymizedSrc the anonymized dataframe
   * @param columnNames the array of column names
   * @return InformationLossBound the measured information loss bound
   */
  def lossMeasure(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      columnNames: Array[String]): InformationLossBound =
    lossMeasure(src, anonymizedSrc, anonymizedSrc, columnNames)

  /**
   * Measures the information loss of anonymized data compared to the original data.
   * 
   * @param src the dataframe
   * @param anonymizedSrc the anonymized dataframe
   * @param suppressedSrc the suppressed dataframe
   * @param columnNames the array of column names
   * @return InformationLossBound the measured information loss bound
   */
  def lossMeasure(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      suppressedSrc: DataFrame,
      columnNames: Array[String]): InformationLossBound

  /**
   * Gets the number of the equivalence class which is composed of referring
   * to the columns of quasi-identifiers.
   * 
   * @param src the dataframe
   * @param columnNames the array of quasi-identifier columns
   * @return Long the number of equivalence classes
   */
  def getNumEquivalence(
      src: DataFrame,
      columnNames: Array[String]): Double = 
    src.select(columnNames.map(x => col(x)): _*).dropDuplicates.count.toDouble
    
  /**
   * Converts all the contents of a specific column to the array of string.
   * 
   * @param src the dataframe
   * @param columnName the column name
   * @return Array[String] the number of equivalence classes
   */
  def convertColumntoArray(
      src: DataFrame,
      columnName: String): Array[String] = 
    src.select(columnName).rdd.map(r => r(0).toString).collect()
 
}