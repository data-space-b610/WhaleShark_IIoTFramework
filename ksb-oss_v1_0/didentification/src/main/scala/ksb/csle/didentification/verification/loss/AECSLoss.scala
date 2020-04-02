package ksb.csle.didentification.verification.loss

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.utilities._
import ksb.csle.didentification.interfaces._

/**
 * This class implements the loss measure method named the AECS (Average
 * Equivalence Class Size).
 */
class AECSLoss extends InformationLoss {
  
  /**
   * Measures the information loss of anonymized data compared to the original data.
   * 
   * @param src the dataframe
   * @param anonymizedSrc the anonymized dataframe
   * @param suppressedSrc the suppressed dataframe
   * @param columnNames the array of column names
   * @return InformationLossBound the measured information loss bound
   */
  override def lossMeasure(
      src: DataFrame,
      anonymizedSrc: DataFrame,
      suppressedSrc: DataFrame,
      columnNames: Array[String]): InformationLossBound = {
    val srcLoss = getLossMeasure(src, columnNames)
    val anoLoss = getLossMeasure(anonymizedSrc, columnNames)
    val supLoss = getLossMeasure(suppressedSrc, columnNames)
    
    // last two arguments should be modified
    new InformationLossBound(
        supLoss, 
        anoLoss, 
        1.0 - srcLoss/supLoss, 
        1.0 - srcLoss/anoLoss)
  }
  
  /**
   * Measures the AECS loss of 'src' dataframe
   * 
   * @param src the dataframe
   * @param columnNames the array of column names
   * @return Double the measured AECS loss
   */
  private def getLossMeasure(
      src: DataFrame,
      columnNames: Array[String]): Double = 
    src.count.toDouble / getNumEquivalence(src, columnNames)
  
}
 
object AECSLoss {
  def apply(): AECSLoss = new AECSLoss()
}
