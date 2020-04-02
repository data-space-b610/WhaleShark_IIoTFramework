package ksb.csle.didentification.utilities

import org.apache.spark.sql._
import scala.collection.mutable.Map

/**
 * This object provides some method to swap the records.
 */
object SwappingManager {
  
  /**
   * Gets the list of records [from record, to record] to swap. The number
   * of swapping records is ratio * overall number of records.
   * 
   * @param src Dataframe
   * @param ratio the ratio of records to swap
   * @return List[(Int, Int)] Returns the list of tuple (from record, to record)
   */
  private def getRandomSwappingList(src: DataFrame, ratio: Double)
      : List[(Int, Int)] = {
    var swapList = List[(Int, Int)]()
    for(i <- 0 until (src.count * ratio / 2).toInt) {
      val from = scala.util.Random.nextInt(src.count.toInt)
      val to = scala.util.Random.nextInt(src.count.toInt)
      swapList = swapList :+ (from, to)
      swapList = swapList :+ (to, from)
    }
    swapList.sortBy(_._1)
  }
  
  /**
   * Gets the map of records [The index of row to be swapped, the contents
   * of records to swap] by referring to the given swap list information.
   * 
   * @param src Dataframe
   * @param swapList the list of records to swap
   * @return Map[Int, Row] Returns the map [Int: the row index, Row:
   * the row to swap]
   */
  def getSwappingRecords(src: DataFrame, swapList: List[(Int, Int)])
      : Map[Int, Row] = {
    val swapRecords = Map[Int, Row]()
    src.rdd.zipWithIndex.collect.map(row => 
      swapList.map(list => 
        if(list._1 == row._2) swapRecords += (list._1 -> row._1))
    )
    (swapRecords)
  }
  
  /**
   * Swaps some records of 'columnNames' columns in 'src' dataframe. 
   * 
   * @param src Dataframe
   * @param columnNames The array of column names to apply swapping function
   * @param ratio The ratio of records to swap
   * @return DataFrame The swapped records
   */
  def randomSwappingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      ratio: Double): DataFrame = 
    swappingDataFrame(src, columnNames, getRandomSwappingList(src, ratio))
  
  /**
   * Swaps records of 'columnNames' columns in 'src' dataframe using the
   * given the list of swap records. 
   * 
   * @param src Dataframe
   * @param columnNames The array of column names to apply swapping function
   * @param swapList The list of records [from record, to record] to swap 
   * @return DataFrame The swapped records
   */
  def swappingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      swapList: List[(Int, Int)]): DataFrame = {
    swapList.map(swap => println(swap))
    val swapRecordLists = getSwappingRecords(src, swapList)
    val result = src.rdd.zipWithIndex.collect.map(row => {
      var update = row._1
      swapList.map(list => 
        if(list._1 == row._2) {
          val orgRecord = swapRecordLists.get(list._1).orNull
          val newRecord = swapRecordLists.get(list._2).orNull
          update = updateRecord(orgRecord, newRecord, columnNames)
        })
      (update)
    })
    
    val rdd = src.sparkSession.sparkContext.parallelize(result)
    src.sqlContext.createDataFrame(rdd, src.schema)
  }
  
  /**
   * Updates the specified columns in 'orgRecord' record to the ones in
   * 'newRecord' record.
   * 
   * @param orgRecord The original record
   * @param newRecord The new record
   * @param columnNames The columns to be swapped 
   * @return Row The swapped row
   */
  def updateRecord(
      orgRecord: Row, 
      newRecord: Row, 
      columnNames: Array[String]): Row = {
    var rowSeq = Seq[Any]()
    for(col <- orgRecord.schema.fieldNames)
      if(columnNames.contains(col)) rowSeq = rowSeq :+ newRecord.getAs(col)
      else rowSeq = rowSeq :+ orgRecord.getAs(col)

    Row.fromSeq(rowSeq)
  }
  
  
  /**
   * Gets the specified row using index.
   * 
   * @param src DataFrame
   * @param index the index of row to get
   * @return Row The specified row
   */
  def getRecordUsingIndexFromDF(src: DataFrame, index: Int): Row = {
    src.rdd.zipWithIndex().filter(_._2 == index).map(_._1).first
  }
  
}