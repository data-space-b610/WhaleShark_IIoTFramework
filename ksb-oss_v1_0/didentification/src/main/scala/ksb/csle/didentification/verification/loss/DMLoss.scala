package ksb.csle.didentification.verification.loss

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.utilities._
import ksb.csle.didentification.interfaces._

/**
 * This class implements the loss measure method named the DM (Discern Metrics).
 */
class DMLoss extends InformationLoss {
  
  /**
   * Measures the information loss bound of anonymized data
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
        1.0 - supLoss/srcLoss, 
        1.0 - anoLoss/srcLoss)
  }
  
  /**
   * Measures the DM (Discern Metric) loss of 'src' dataframe
   * 
   * @param src the dataframe
   * @param columnNames the array of column names
   * @return Double the measured DM loss
   */
  private def getLossMeasure(
      src: DataFrame,
      columnNames: Array[String]): Double = {
    var loss = 0.0
    val eqAbstractInfo = 
      EquivalClassManager.getECAbstractionInfo(src, columnNames)
    eqAbstractInfo.map(eqInfo => {
      if(isSuppressedEqClass(eqInfo._1)) loss += src.count * eqInfo._2
      else loss += eqInfo._2 * eqInfo._2
    })
    
    (loss)
  }
  
  /**
   * Checks the given sequence of quasi-identifiers to be suppressed or not.
   * 
   * @param quasiIdentifiers the sequence of quasi-identifiers
   * @return Boolean return true if the quasi-identifiers are suppressed
   */
  private def isSuppressedEqClass(quasiIdenfiers: Seq[Any]): Boolean = {
    quasiIdenfiers.map(quasi => 
      if(quasi != "*") return false)
      
    return true
  }
  
}
 
object DMLoss {
  def apply(): DMLoss = new DMLoss()
}
