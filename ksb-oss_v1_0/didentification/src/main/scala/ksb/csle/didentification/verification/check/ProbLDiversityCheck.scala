package ksb.csle.didentification.verification.check

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

/**
 * This class checks whether the anonymized data satisfies the probabilistic
 * L-diversity constraint. Note that this class checks the l-diversity
 * value in an single equivalence class, not the dataframe.
 */
class ProbLDiversityCheck extends BaseLDiversityCheck {

  /**
   * The probabilistic L-diversity method checks the probability of the most
   * frequent attribute values in the equivalence class to be less than 1/l.
   * The table is said to be probabilistic l-diverse if every equivalence satisfy
   * the probabilistic l-diverse.
   *
   * @param sensList the list of sensitive attributes
   * @return Boolean return true when satisfying privacy policy
   */
  override def getLDiversityValue(
      eqTable: Map[List[Any], Long]): Double = {
    val maxFrequency = eqTable.maxBy(_._2)
    (maxFrequency._2 / eqTable.values.sum.toDouble)
  }

}

object ProbLDiversityCheck {
  def apply(): ProbLDiversityCheck = new ProbLDiversityCheck
}
