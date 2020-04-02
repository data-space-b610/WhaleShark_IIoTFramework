package ksb.csle.didentification.verification.check

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

/**
 * This class checks whether the anonymized data satisfies the entropy
 * L-diversity constraint.Note that this class checks the l-diversity
 * value in an single equivalence class, not the dataframe.
 */
class EntropyLDiversityCheck extends BaseLDiversityCheck {

  /**
   * The entropy L-diversity constraint calculates the entropy of sensitive
   * attributes while considering their distributions in an equivalence class.
   * A table is Entropy l-diverse if for every equivalence class E
   * -sigma_{s \in S}p(E,s)log(p(E,s)) >= log(l)
   * where p(E,s) is the fraction of tuples in the equivalence class E
   * with sensitive attribute value equal to s.
   *
   * @param sensList the list of sensitive attributes
   * @return Boolean return true when satisfying privacy policy
   */
  override def getLDiversityValue(
      eqTable: Map[List[Any], Long]): Double = {
    val eqSum = eqTable.values.sum.toDouble
    var entropy = 0.0
//    println(eqTable)
    eqTable.map(entry => {
      val prob = entry._2 / eqSum
      entropy += (prob * scala.math.log(prob))
//      print(prob, entropy)
    })
//    println(-1 * entropy)
    (-1 * entropy)
  }

}

object EntropyLDiversityCheck {
  def apply(): EntropyLDiversityCheck = new EntropyLDiversityCheck
}
