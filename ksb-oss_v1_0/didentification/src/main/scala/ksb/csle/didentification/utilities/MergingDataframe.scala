package ksb.csle.didentification.utilities

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * This trait provides the statistics related functions such as
 * min, max, avg, std, and # of records. 
 */
object MergingDataframe {
  
  /**
   * Gets the minimum value among this column
   * 
   * @param src Dataframe
   * @param columnName Column
   * @return Double Minimum value
   */
  def merging(df1: DataFrame, df2: DataFrame): DataFrame = 
    merging(df1, Seq.range(0, df1.columns.length), df2, Seq.range(0, df2.columns.length))

  def merging(df1: DataFrame, colIds: Seq[Int], df2: DataFrame): DataFrame = 
    merging(df1, colIds, df2, Seq.range(0, df2.columns.length))
    
  def merging(df1: DataFrame, df2: DataFrame, colIds: Seq[Int]): DataFrame = 
    merging(df1, Seq.range(0, df1.columns.length), df2, colIds)
    
  def merging(
      df1: DataFrame,
      colIds1: Seq[Int],
      df2: DataFrame,
      colIds2: Seq[Int]): DataFrame = {
    require(df1.count == df2.count)

    val selectDF1 = getSelectDF(df1, colIds1)
    val selectDF2 = getSelectDF(df2, colIds2)
    val dfSeq1 = selectDF1.collect.map(_.toSeq)
    val dfSeq2 = selectDF2.collect.map(_.toSeq)
        
    var result = List[Row]()
    for(i <- 0 until selectDF1.count.toInt) 
      result = result.+:(Row.fromSeq(dfSeq1(i) ++ dfSeq2(i)))
    
    val rdd = selectDF1.sparkSession.sparkContext.parallelize(result)
    var newSchema = Seq[StructField]()
    selectDF1.schema.map(x => newSchema = newSchema :+ new StructField(x.name, x.dataType))
    selectDF2.schema.map(x => newSchema = newSchema :+ new StructField(x.name, x.dataType))
//    println("COLUMN LENGTH: " + (dfSeq1(0) ++ dfSeq2(0)).length)
    
    df1.sparkSession.createDataFrame(rdd, StructType(newSchema))
  }

  private def getSelectDF(src: DataFrame, colId: Seq[Int]): DataFrame = {
    if(colId.length == 0) src
    else src.select(colId map src.columns map col: _*)
  }
  
}