package ksb.csle.didentification.verification.check

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * This class checks whether the anonymized data satisfies the common
 * K-anonymity constraint.
 */
class KAnonymityCheck {

  /**
   * Returns the k-anonymity value.
   *
   * @param src The source dataframe
   * @param columnNames The array of column names
   * @return Double return the k-anonymity value.
   */
  def getKAnonymityValue(
      src: DataFrame,
      columnNames: Array[String]): Double = {
    var kAnonymityValue = Double.MaxValue
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(group => {
        val nTuplesInGroup = group.getAs[Long](group.length-1).toDouble
        if(nTuplesInGroup < kAnonymityValue)
          kAnonymityValue = nTuplesInGroup
      })

    (kAnonymityValue)
  }

  /**
   * Checks the calculated k-anonymity value of 'src' dataframe is bigger
   * than the given k-anonymity value.
   *
   * @param src The source dataframe
   * @param columnNames The array of column names
   * @param kAnonymityValue The given k-anonymity value
   * @return Boolean return true if the calculated k-anonymity value is bigger
   * than the given one.
   */
  def kAnonymityCheck(
      src: DataFrame,
      columnNames: Array[String],
      kAnonymityValue: Int): Boolean = {
    val kValue = getKAnonymityValue(src, columnNames)
    src.show
    println("K Value: " + kValue + ", Given: " + kAnonymityValue)
    if(kValue > kAnonymityValue) return true
    else return false
  }

}

object KAnonymityCheck {
  def apply(): KAnonymityCheck = new KAnonymityCheck
}
