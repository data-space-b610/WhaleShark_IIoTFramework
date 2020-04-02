package ksb.csle.didentification.verification.check

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable.Map

/**
 * This class is a base class to check whether the anonymized data satisfies
 * the L-diversity constraints. Note that there are some variants of
 * L-diversity. Currently, common l-diversity, entropy l-diversity,
 * probabilistic l-diversity, and recursive C-L diversity are implemented.
 */
abstract class BaseLDiversityCheck {

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
  def getLDiversityValue(
      src: DataFrame,
      columnNames: Array[String],
      sens: Array[String]): Double = {
    var minlDiversityValue = Double.MaxValue
    var preQuasiCol = List[Any]()
    var eqLDiversityTable = Map[List[Any], Long]()
    src.groupBy((columnNames ++ sens).map(x => col(x)): _*).count
      .rdd.collect.map(row => {
        var curQuasiCol = List[Any]()
        columnNames.map(x => curQuasiCol = curQuasiCol :+ row.getAs(x))

        if(preQuasiCol.length == 0) preQuasiCol = curQuasiCol
        
        val sensVal = sens.map(x => row.getAs[Any](x)).toList
        if(preQuasiCol != curQuasiCol) { // in case of next equivalence class
          val lValue = getLDiversityValue(eqLDiversityTable)
          if(lValue < minlDiversityValue)
            minlDiversityValue = lValue

          preQuasiCol = curQuasiCol
          eqLDiversityTable.clear
        }
        eqLDiversityTable += (sensVal -> row.getAs[Long](row.length-1))
      })
      
    // the last equivalence class
    val lValue = getLDiversityValue(eqLDiversityTable)
    if(lValue < minlDiversityValue)
      minlDiversityValue = lValue
        
    (minlDiversityValue)
  }
  
  /**
   * The common L-diversity method just checks whether the number of sensitive
   * attributes in single equivalence class is greater than given L-constraints.
   *
   * @param src The anonymized dataframe
   * @param sensList the list of sensitive attributes in single equivalence class
   * @return Double return the l-diversity value
   */
  def getLDiversityValue(
      eqTable: Map[List[Any], Long]): Double

  /**
   * Checks the anonymized dataframe satisfies the given l-Diversity constraint.
   *
   * @param src The anonymized dataframe
   * @param columnName the array of the column name with quasi-identifiers
   * @param sensList the array of the column name with sensitive attributes
   * @param lDiversityValue the given l-diversity constraint
   * @return Boolean return true if satisfying the given l-diversity constraint
   */
  def lDiversityCheck(
      src: DataFrame,
      columnNames: Array[String],
      sens: Array[String],
      lDiversityValue: Double): Boolean = {
    val lValue = getLDiversityValue(src, columnNames, sens)
    println("L Value: " + lValue + " Given: " + lDiversityValue)
    if(lValue >= lDiversityValue) return true
    else return false
  }

}
