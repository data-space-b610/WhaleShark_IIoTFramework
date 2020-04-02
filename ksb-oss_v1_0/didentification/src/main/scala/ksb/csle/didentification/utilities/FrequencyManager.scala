package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This object manages the frequency of values in the specified column.
 */
object FrequencyManager {
  
  private val boxplotCoeff = 0.5
  private val zscoreCoeff = 1.5

  /**
   * Gets the entry which has the highest frequency among
   * the 'columnName' column in 'src' dataframe.
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Any the column entry with highest frequency
   */
  def getHighestFrequencyEntry(
      src: DataFrame, 
      columnName: String): Any =
    makeFrequencyTable(src, columnName).maxBy(_._2)._1

  def getLeastFrequencyEntry(
      src: DataFrame, 
      columnName: String): Any =
    makeFrequencyTable(src, columnName).minBy(_._2)._1

  /**
   * Makes the frequency table which describes the frequencies of all 
   * elements of the 'columnname' column in src dataframe as a form of map 
   * [element, the number of occurrences].
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Map[Any, Long]
   */
  def makeFrequencyTable(
      src: DataFrame, 
      columnName: String): Map[Any, Long] = {
    val table = Map[Any, Long]()
//    src.show
//    println("N:" + columnName)
//    src.groupBy(columnName).count.show()
    src.groupBy(columnName).count.rdd.collect.map(x =>
        table += (x.get(0) -> x.get(1).asInstanceOf[Long]))
      
    table
  }

  /*
   * Makes the frequency table which describes the frequencies of all elements
   * of the array of columns in src dataframe as a form of map 
   * [[element1, element 2, ..., element n], the number of occurrences].
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Map[Any, Long]
   */
  def makeFrequencyTable(
      src: DataFrame, 
      columnNames: Array[String]): Map[List[Any], Long] = {
    val table = Map[List[Any], Long]()
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(row => {
        val elements: List[Any] = row.toSeq.toList.dropRight(1)
        val freqCount: Long = row.getAs[Long](row.length-1)
        table += (elements -> freqCount)
      })
      
    table
  }
  
  /**
   * Firstly, get the frequency info about the 'columnName' column in 'src'
   * dataframe. Based on the frequency, the less frequent tuples
   * are described by z-score outlier method, and then return them.
   * [less frequent element, the number of occurrences].
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Map[Any, Long]
   */
  def makeZscoreOutlierBasedFrequency(
      src: DataFrame, 
      columnName: String): Map[Any, Long] = {
    val table = makeFrequencyTable(src, columnName)
    makeZscoreOutlierBasedFrequency(src, table)
  }

   /**
   * Same as makeZscoreOutlierBasedFrequency(src, columnName), but the
   * columns to be investigated are given by array of columns
   * [less frequent 'array of element', the number of occurrences].
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Map[List[Any], Long]
   */
  def makeZscoreOutlierBasedFrequency(
      src: DataFrame, 
      columnNames: Array[String]): Map[List[Any], Long] = {
    val table = makeFrequencyTable(src, columnNames)
    makeZscoreOutlierBasedFrequency(src, table)
  }

  private def makeZscoreOutlierBasedFrequency[T](
      src: DataFrame, 
      table: Map[T, Long]): Map[T, Long] = {
    val freqStat = src.sparkSession.sparkContext.parallelize(
        table.toSeq.map(x => x._2)).stats
    val lowerBound = freqStat.mean - zscoreCoeff * math.sqrt(freqStat.variance)
    val outlierTable = table.filter(x => x._2 < lowerBound)
    
    println(outlierTable)
    (outlierTable)
  }
  
  /**
   * Same as makeZscoreOutlierBasedFrequency(src, columnName), but the
   * method to discriminate outliers is boxplot.
   * [less frequent element, the number of occurrences].
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Map[Any, Long]
   */
  def makeBoxplotOutlierBasedFrequency(
      src: DataFrame, 
      columnName: String): Map[Any, Long] = {
    val table = makeFrequencyTable(src, columnName)
    makeBoxplotOutlierBasedFrequency(src, table)
  }

  /**
   * Same as makeBoxplotOutlierBasedFrequency(src, columnName), but the
   * columns to be investigated are given by array of columns
   * [less frequent 'array of element', the number of occurrences].
   * 
   * @param src Dataframe
   * @param columnName Column to be investigated
   * @return Map[List[Any], Long]
   */
  def makeBoxplotOutlierBasedFrequency(
      src: DataFrame, 
      columnNames: Array[String]): Map[List[Any], Long] = {
    val table = makeFrequencyTable(src, columnNames)
    makeBoxplotOutlierBasedFrequency(src, table)
  }

  private def makeBoxplotOutlierBasedFrequency[T](
      src: DataFrame,
      table: Map[T, Long]): Map[T, Long] = {
    import src.sparkSession.implicits._
    
    val df = table.values.toList.toDF("freq")
    val quantiles = df.stat.approxQuantile("freq", Array(0.25, 0.75), 0.0)
    val IQR = quantiles(1) - quantiles(0)
    val lowerBound = quantiles(0) - boxplotCoeff * IQR
    val outlierTable = table.filter(x => x._2 < lowerBound)
    
    println(outlierTable)
    (outlierTable)
  }
  
}