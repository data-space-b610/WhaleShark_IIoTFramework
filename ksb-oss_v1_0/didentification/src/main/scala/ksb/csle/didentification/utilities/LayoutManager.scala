package ksb.csle.didentification.utilities

import org.apache.spark.sql._

/**
 * This object manages the layout information about columns in dataframe.
 */
object LayoutManager {
  
  /**
   * Checks the column is string type or not.
   * 
   * @param src Dataframe
   * @param columnName Column to check
   * @return Boolean Return true if the column is string type
   */
  def isStringColumn(
      src: DataFrame, 
      columnName: String): Boolean = {
    var result = false
    src.select(columnName).rdd.collect.map(row => 
      if(!row.get(0).toString.matches(".*\\d+.*")) result = true)
    
    (result)
  }

}