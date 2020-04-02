package ksb.csle.didentification.verification.risk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This class measures the re-identification risk of the given data frame 
 * on the basis of the sample uniqueness method.
 */
class GlobalRiskMeasure extends ReIdentificationRisk {
  
  /**
   * Measures the re-identification risks based on the sample uniqueness.
   * 
   * @param src the dataframe to measure the risk
   * @param columnNames the array of column names of quasi-identifiers. The
   * combination key is made by cross-tabulating these variables.
   * @return Double the measured re-identification risk
   */
  override def riskMeasure(
      src: DataFrame,
      columnNames: Array[String]): Double = {
    var risk = 0.0
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(group => 
        risk += (1 / group.getAs[Long](group.length-1).toDouble))
        
    val nGroups = 
      src.select(columnNames.map(x => col(x)): _*).dropDuplicates.count.toDouble
    (risk / nGroups)
  }  

}

object GlobalRiskMeasure {
  def apply(): GlobalRiskMeasure = new GlobalRiskMeasure()
}
