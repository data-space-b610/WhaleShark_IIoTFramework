package ksb.csle.didentification.verification.check

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

import ksb.csle.didentification.utilities.FrequencyManager

/**
 * This class is a base class to check whether the anonymized data satisfies
 * the T-closeness constraints.
 */
class CategoryTClosenessCheck {
  
  /**
   * Checks the anonymized dataframe satisfies the given l-Diversity constraint.
   *
   * @param src The anonymized dataframe
   * @param columnName the array of the column name with quasi-identifiers
   * @param sensList the array of the column name with sensitive attributes
   * @param lDiversityValue the given l-diversity constraint
   * @return Boolean return true if satisfying the given l-diversity constraint
   */
  def getTClosenessValue(
      src: DataFrame,
      columnNames: Array[String],
      sens: Array[String]): Double = {
    var tCloseness = 0.0
    val allTClosenessTable = getOverallTClosenessTable(src, sens)
    var preQuasiCol = List[Any]()
    var eqTClosenessTable = Map[List[Any], Long]()
    src.groupBy((columnNames ++ sens).map(x => col(x)): _*).count
      .rdd.collect.map(row => {
        var curQuasiCol = List[Any]()
        columnNames.map(x => curQuasiCol = curQuasiCol :+ row.getAs(x))

        if(preQuasiCol.length == 0) preQuasiCol = curQuasiCol
        
        val sensVal = sens.map(x => row.getAs[Any](x)).toList
        if(preQuasiCol != curQuasiCol) {
          tCloseness += getTClosenessValue(eqTClosenessTable, allTClosenessTable)
              
          preQuasiCol = curQuasiCol
          eqTClosenessTable.clear
        }
        eqTClosenessTable += (sensVal -> row.getAs[Long](row.length-1))
      })
      
    tCloseness += getTClosenessValue(eqTClosenessTable, allTClosenessTable)
    
    (tCloseness)
  }
  
  def getOverallTClosenessTable(
      src: DataFrame,
      sens: String): Map[List[Any], Long] = { 
    getOverallTClosenessTable(src, Array(sens))
  }
  
  def getOverallTClosenessTable(
      src: DataFrame,
      sens: Array[String]): Map[List[Any], Long] = { 
    FrequencyManager.makeFrequencyTable(src, sens)
  }
  
  def getTClosenessValue(
      eqTable: Map[List[Any], Long], 
      allTable: Map[List[Any], Long]): Double = {
    var tValue = 0.0
    println(eqTable.size + " " + allTable.size)
    val totalSum = allTable.values.sum.toDouble
    val eqSum = eqTable.values.sum.toDouble
    println(eqTable)
    allTable.map(entry => {
      val eqEntryCount = eqTable.get(entry._1).getOrElse(0).toString().toLong
      println(entry._1 + " " + entry._2 + " " + totalSum + " " + eqEntryCount + " " + eqSum + " ")
      tValue += Math.abs(entry._2/totalSum - eqEntryCount/eqSum)
    })
    
    (tValue / 2)
  }
  
  /**
   * The common L-diversity method just checks whether the number of sensitive
   * attributes is greater than given L-constraints.
   * Generally, an equivalence class is l-diverse if contains at least
   * 'l' well-represented values for the sensitive attribute. A table
   * is l-diverse if every equivalence is l-diverse
   *
   * @param src The anonymized dataframe
   * @param columnName the array of the column name with quasi-identifiers
   * @param sensList the array of the column name with sensitive attributes
   * @return Double return the l-diversity value
   */
  def tClosenessCheck(
      src: DataFrame,
      columnNames: Array[String],
      sens: Array[String],
      tClosenessValue: Double): Boolean = {
    val tValue = getTClosenessValue(src, columnNames, sens)
    println("T Value: " + tValue + " Given: " + tClosenessValue)
    if(tValue > tClosenessValue) return false
    else return true
  }
  
}

object CategoryTClosenessCheck {
  def apply(): CategoryTClosenessCheck = new CategoryTClosenessCheck()
}

