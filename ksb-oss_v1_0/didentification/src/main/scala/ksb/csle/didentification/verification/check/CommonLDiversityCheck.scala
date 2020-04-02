package ksb.csle.didentification.verification.check

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

/**
 * This class checks whether the anonymized data satisfies the common
 * L-diversity constraint. Note that this class checks the l-diversity
 * value in an single equivalence class, not the dataframe.
 */
class CommonLDiversityCheck extends BaseLDiversityCheck {

  /**
   * Gets the l-diversity value of this equivalence class.
   * Note that this function checks the l-diversity value in an single
   * equivalence class, not the dataframe. that is, the 'senslist' is the
   * list of senstivie contents in an single equivalence class.
   *
   * @param sensList The list of sensitive contents in the equivalence class
   * @return Double The l-diversity value
   */
  override def getLDiversityValue(
      eqTable: Map[List[Any], Long]): Double = {
//    println(eqTable)
    eqTable.size
  }
  
}

object CommonLDiversityCheck {
  def apply(): CommonLDiversityCheck = new CommonLDiversityCheck
}
