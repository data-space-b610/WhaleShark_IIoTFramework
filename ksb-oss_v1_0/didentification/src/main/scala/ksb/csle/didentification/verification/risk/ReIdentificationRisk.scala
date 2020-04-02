package ksb.csle.didentification.verification.risk

import scala.collection.mutable.Map

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * This abstract class defines some functions for measuring the re-identification
 * risk shared by 'sample uniqueness', 'population uniqueness', 'attribute risk'
 * and other re-identification measuring methods.
 * To Be Defined for future
 */
class ReIdentificationRisk extends Serializable {
  
  def riskMeasure(
      src: DataFrame,
      columnNames: Array[String]): Double = {
    var maxRisk = 0.0
    var risk = 0.0
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(group => {
        risk = (1 / group.getAs[Long](group.length-1).toDouble)
        if(maxRisk < risk) maxRisk = risk
        })
    
    (maxRisk)
  }  
  
  /**
   * Makes the list of the risk for all individuals
   * 
   * @param src given Dataframe
   * @param columnName the path of file
   * @param delimiter the column separator
   * @return DataFrame
   */
  def makeListofIndividualRisk(
      src: DataFrame,
      columnNames: Array[String]): Map[List[Any], Long] = {
    val listofIndividualRisk = Map[List[Any], Long]()
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(group => {
        listofIndividualRisk += (convertRowtoList(group) -> 
          group.getAs[Long](group.length-1))
      })
      
    (listofIndividualRisk)
  }
  
  private def convertRowtoList(row: Row): List[Any] = {
    var rowToList = List[Any]()
    for(i <- 0 until row.length-1) rowToList = rowToList :+ row.get(i)
    (rowToList)
  }
  
}

object ReIdentificationRisk {
  def apply(): ReIdentificationRisk = new ReIdentificationRisk()
}
