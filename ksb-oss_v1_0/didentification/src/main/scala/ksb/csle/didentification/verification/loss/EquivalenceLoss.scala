package ksb.csle.didentification.verification.loss

import scala.collection.mutable.Map
import org.apache.spark.sql._

import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.utilities._
import ksb.csle.didentification.interfaces._

/**
 * This class implements the loss measure method named as Euqivalence Loss.
 */
class EquivalenceLoss extends InformationLoss {
  
  /**
   * Measures the information loss based on the number of equivalence classes.
   * 
   * @param src the original dataframe
   * @param anonymizedSrc the generalized dataframe
   * @param columnNames the array of column names of quasi-identifiers. The
   * combination key is made by cross-tabulating these variables.
   * @return Double the measured information loss
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
   * Measures the equivalence class loss of 'src' dataframe
   * 
   * @param src the dataframe
   * @param anonymizedSrc the anonymized dataframe
   * @param columnNames the array of column names
   * @return Double the measured equivalence loss
   */
  private def getLossMeasure(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      columnNames: Array[String]): Double = {
    val oriEquival = getNumEquivalence(src, columnNames)
    val anoEquival = getNumEquivalence(anonymizedSrc, columnNames)
    
    1.0 - (anoEquival / oriEquival)
  }
}

object EquivalenceLoss {
  def apply(): EquivalenceLoss = new EquivalenceLoss()
}
