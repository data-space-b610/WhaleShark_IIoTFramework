package ksb.csle.didentification.interfaces

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe

trait DeAnonymizer {
  
  /**
   * De-anonymizes the anonymized dataframe by using the given heuristic table
   * 
   * @param src Dataframe to de-anonymize
   * @param columnName Column name to be de-anonymized
   * @param heuristicTable The heuristic table which was used to anonymize
   * the records 
   * @return DataFrame The de-anonymized dataframe
   */
  def deAnonymize(
      src: DataFrame, 
      columnName: String, 
      heuristicTable: Map[String, String]): DataFrame = {
    def heursticDeAnonymize: (String => String) =
      value => getKeyFromTableUsingValue(heuristicTable, value)

    val deAnonymizeUdf = udf(heursticDeAnonymize)
    src.withColumn(columnName, deAnonymizeUdf(src.col(columnName)))
  }

  /**
   * get the original data by searching the table using the de-anonymized data
   * 
   * @param heuristicTable The heuristic table which was used to anonymize
   * the records
   * @param value the value to de-anonymize
   * @return String the de-anonymized value
   */
  private def getKeyFromTableUsingValue(
      heuristicTable: Map[String, String],
      value: String):String = {
    heuristicTable.map(x => if(value == x._2) return x._1)
    
    return "None"
  }
  
}