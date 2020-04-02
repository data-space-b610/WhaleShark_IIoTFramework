package ksb.csle.didentification.verification.check

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ListBuffer

/**
 * This class checks whether the anonymized data satisfies the recursive
 * L-diversity constraint.Note that this class checks the l-diversity
 * value in an single equivalence class, not the dataframe.
 */
class RecursiveLDiversityCheck {

  /**
   * The recursive L-diversity constraint checks the following conditions.
   *
   * In a given equivalence class, let r_i denote the number of times the
   * i-th most frequent sensitive value appears in that equivalence class.
   * Given a constant c, the equivalence class satisfies recursive
   * (c,l)-diverse if r_1 < c(r_l + r_(l+1) + r_m)
   *
   * A table satisfies recursive (c,l)-diversity if every equivalence class
   * satisfies recursive l-diversity.
   *
   * @param sensList the list of sensitive attributes
   * @return Boolean return true when satisfying privacy policy
   */
  def lDiversityCheck(
      sensList: List[Any],
      cValue: Int,
      lValue: Double): Boolean = {
    if(sensList.distinct.size < lValue) return false

    val orderedSensList =
      sensList.distinct.sortBy(attr => sensList.count(_ == attr))

    val highestFreqSensVal = orderedSensList(orderedSensList.size - 1)
    val highestFreq = sensList.count(_ == highestFreqSensVal)
    
    var sum = -1 * highestFreq
    orderedSensList.map(attr => sum += sensList.count(_ == attr))
    
    if(highestFreq < cValue * sum) true
    else false
  }

  /**
   * Checks the anonymized dataframe satisfies the given l-Diversity constraint.
   *
   * @param src The anonymized dataframe
   * @param columnName the array of the column name with quasi-identifiers
   * @param sensList the array of the column name with sensitive attributes
   * @param lValue the l value of given (c,l)-diversity constraint
   * @param cValue the c value of given (c,l)-diversity constraint
   * @return Boolean return true if satisfying the given l-diversity constraint
   */
  def lDiversityCheck(
      src: DataFrame,
      columnNames: Array[String],
      sens: Array[String],
      lValue: Double,
      cValue: Int): Boolean = {
    var preQuasiCol = List[Any]()
    var sensList = List[String]()
    src.orderBy((columnNames ++ sens).map(x => col(x)): _*)
      .rdd.collect.map(row => {
        var curQuasiCol = List[Any]()
        columnNames.map(x => curQuasiCol = curQuasiCol :+ row.getAs(x))

        if(preQuasiCol.length == 0) preQuasiCol = curQuasiCol
        
        val sensVal = sens.map(x => row.getAs[String](x)).mkString(",")
        if(preQuasiCol == curQuasiCol) { // same equivalence class
          sensList = sensList :+ sensVal
        } else { // in case of next equivalence class
          if(!lDiversityCheck(sensList, cValue, lValue)) return false

          preQuasiCol = curQuasiCol
          sensList = List[String]()
          sensList = sensList :+ sensVal
        }

      })
    
    // check the privacy of last equivalence class
    if(!lDiversityCheck(sensList, cValue, lValue)) return false
      
    return true
  }
  
}

object RecursiveLDiversityCheck {
  def apply(): RecursiveLDiversityCheck = new RecursiveLDiversityCheck
}
