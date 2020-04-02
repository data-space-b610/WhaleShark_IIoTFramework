package ksb.csle.didentification.utilities

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * This trait provides the statistics related functions such as
 * min, max, avg, std, and # of records. 
 */
object SelectDataframe {
  
  /**
   * Gets the minimum value among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return Double Minimum value
   */
  def dropFirstNRows(src: DataFrame, nRows: Long): DataFrame = {
    val filteredRdd = src.rdd.zipWithIndex().collect { 
      case (r, i) if i >= nRows => r 
    }
    src.sparkSession.createDataFrame(filteredRdd, src.schema)
  }

  def dropTailNRows(src: DataFrame, nRows: Int): DataFrame = {
    val rdd = src.sparkSession.sparkContext.parallelize(
        src.take(src.count.toInt - nRows))
    src.sparkSession.createDataFrame(rdd, src.schema)
  }
    
}