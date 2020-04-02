package ksb.csle.didentification.verification.loss

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.utilities._
import ksb.csle.didentification.interfaces._

/**
 * This class implements the loss measure method named as hierarchical tree loss.
 * To Be Modified for future
 */
class HierarchicalTreeLoss(hier: Map[FieldInfo, ColumnHierarchy])
  extends InformationLoss {
  
  var h: Map[FieldInfo, ColumnHierarchy] = hier
  
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
    
    /*new InformationLossBound(
        supLoss, 
        anoLoss,
        supLoss / suppressedSrc.count() / columnNames.length,
        anoLoss / anonymizedSrc.count() / columnNames.length)*/
    new InformationLossBound(supLoss, anoLoss, supLoss/srcLoss, anoLoss/srcLoss)
  }
  
  /**
   * Measures the hierarchical tree loss of 'src' dataframe
   * 
   * @param src the dataframe
   * @param columnNames the array of column names
   * @return Double the measured DM loss
   */
  private def getLossMeasure(
      src: DataFrame,
      columnNames: Array[String]): Double = {
    val eqClasses = EquivalClassManager.getEquivalenceClasses(src, h)
    var loss = 0.0
    src.select(columnNames.map(x => col(x)): _*)
       .rdd.collect.map(row => loss = loss + getTreeLoss(eqClasses, row))
       
    (loss)
  }
          
  /**
   * Measures the hierarchical tree loss by referring to 
   * the given Map[Seq[Any], EquivalenceClass] 
   * 
   * @param Map[Seq[Any], EquivalenceClass] 
   * @return Double the measured tree loss
   */
  private def getTreeLoss(
      eq: Map[Seq[Any], EquivalenceClass], row: Row): Double = {
    var loss = 0.0
    eq.get(row.toSeq) match {
      case Some(eqClass) => {
        eqClass.latticeEntry.map(latticeValue => loss = loss + 
          latticeValue.currentDepth.toDouble / latticeValue.maxDepth)
      }
      case _ => loss = 0.0
    }
    (loss)
  }
}

object HierarchicalTreeLoss {
  def apply(h: Map[FieldInfo, ColumnHierarchy]): HierarchicalTreeLoss = new HierarchicalTreeLoss(h)
}
