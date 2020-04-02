package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe

object SuppressingManager {
  
  /**
   * In general, satisfying k-anonymity in global domain generalization is not 
   * easy because generalization is equally applied on all tuples of a column
   * (that is, column-based generalization). Compared to this, local domain
   * generalization only applies on tuples on an equivalence class (equivalence-based).
   * Accordingly, in global-domain generalization, a large number of tuples may
   * violate the privacy policy, so some specific algorithms support suppressing
   * techniques which changes the tuples does not satisfying privacy policy into '*'.
   * This function reads the 'src' dataframe, configures the tuple lists to be
   * suppressed, and then returns the new dataframe including suppressed tuples. 
   *
   * @param src dataframe to generalize
   * @param columnNames array of column names to be generalization 
   * @return DataFrame the new suppressed dataframe 
   */
  def suppressingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      k: Int): DataFrame = 
    suppressingDataFrame(src, columnNames, getSuppressedTupleListByK(
        src, columnNames, k))
  
  def suppressingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      k: Int,
      suppressThreshold: Double): DataFrame = 
    suppressingDataFrame(src, columnNames, getSuppressedTupleListByKAndRatio(
        src, columnNames, k, suppressThreshold))
  
  def suppressingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      risk: Double): DataFrame = 
    suppressingDataFrame(src, columnNames, getSuppressedTupleListByRisk(
        src, columnNames, risk))

  /*def suppressingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      suppressedList: Map[List[Any], Long]): DataFrame = {
    val result = src.orderBy(columnNames.map(x => col(x)): _*)
      .rdd.collect.map(row => {
        var quasiCol = List[Any]()
        columnNames.map(x => quasiCol = quasiCol :+ row.getAs(x))
        if(suppressedList.contains(quasiCol))
          makeSuppressedQuasiRow(row, columnNames)
        else row
      })

    val rdd = src.sparkSession.sparkContext.parallelize(result)
    src.sqlContext.createDataFrame(rdd, src.schema)
  }*/
  
  def suppressingDataFrame(
      src: DataFrame,
      columnNames: Array[String],
      suppressedList: Map[List[Any], Long]): DataFrame = {
    var supTupleIndex = List[Long]()
    var tupleIndex = 0L
    src.orderBy(columnNames.map(x => col(x)): _*)
      .rdd.collect.map(row => {
        var quasiCol = List[Any]()
        columnNames.map(x => quasiCol = quasiCol :+ row.getAs(x))
        if(suppressedList.contains(quasiCol))
          supTupleIndex = supTupleIndex :+ tupleIndex
        tupleIndex += 1
      })

    suppressing(src, columnNames, supTupleIndex)
  }
  
  private def suppressing(src: DataFrame,
      columnNames: Array[String],
      suppressedTuples: List[Long]): DataFrame = {
    if(suppressedTuples.size == 0) return src
    
    var result = src.orderBy(columnNames.map(x => col(x)): _*)
    columnNames.map(column => {
      result = result.withColumn(column,
        suppressingColumn(result.col(column), suppressedTuples))
    })
    
    (result)
  }
  
  private def suppressingColumn(column: Column,
      suppressedTuples: List[Long]): Column = {
    var tupleIndex = -1
    var suppIndex = 0
    
    def suppColumn: (String => String) = (field => {
      tupleIndex += 1
      if(suppIndex <= suppressedTuples.size &&
          tupleIndex == suppressedTuples(suppIndex)) {
        suppIndex += 1
        "*"
      } else field
    })
    
    val suppresingUdf = udf(suppColumn)
    suppresingUdf(column)
  }
  
  /**
   * Returns the list of tuples to be suppressed under the default
   * condition when the k value of k-anonymity and suppreshold threshold
   * are given by 3 and 0.2, respectively.
   *
   * @param src dataframe to generalize
   * @param columnNames array of column names to be generalization 
   * @return List[Any] the list of tuples to be suppressed
   */
  def getSuppressedTupleList(src: DataFrame,
      columnNames: Array[String]): Map[List[Any], Long] =
    getSuppressedTupleListByKAndRatio(src, columnNames, 3, 0.2)
  
  /**
   * Returns the list of tuples to be suppressed under the condition
   * when the k value of k-anonymity is given by 'k'. All the tuples
   * which violate the condition of k-anonymity are suppressed.
   *
   * @param src dataframe to generalize
   * @param columnNames array of column names to be generalization 
   * @param k the k-value of k-anonymity
   * @return List[Any] the list of tuples to be suppressed
   */
  def getSuppressedTupleListByK(
      src: DataFrame,
      columnNames: Array[String],
      k: Int): Map[List[Any], Long] =
    getSuppressedTupleListByRisk(src, columnNames, 1.0 / k)

  /**
   * Returns the list of tuples to be suppressed under the condition
   * when the re-identification risk is given by 'risk'. All the tuples
   * which has the probability higher than given re-identification risk
   * are suppressed.
   *
   * @param src dataframe to generalize
   * @param columnNames array of column names to be generalization 
   * @param risk the re-identification risk 
   * @return List[Any] the list of tuples to be suppressed
   */
  def getSuppressedTupleListByRisk(
      src: DataFrame,
      columnNames: Array[String],
      risk: Double): Map[List[Any], Long] = {
    val suppList = Map[List[Any], Long]()
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(group => {
        val nTuplesInGroup = group.getAs[Long](group.length-1)
        if(1.0 / nTuplesInGroup < risk) suppList += 
            (group.getValuesMap(columnNames).values.toList -> nTuplesInGroup)
      })

    (suppList)
  }
  
  /**
   * Returns the list of tuples to be suppressed under the condition
   * when the k value of k-anonymity and the suppreshold threshold are
   * given by 'k' and 'suppressRatio'. All the tuples
   * which violate the condition of k-anonymity are suppressed.
   *
   * @param src dataframe to generalize
   * @param columnNames array of column names to be generalization 
   * @param k the k-value of k-anonymity
   * @param suppressRatio the acceptable suppress ratio
   * @return List[Any] the list of tuples to be suppressed
   */
  def getSuppressedTupleListByKAndRatio(
      src: DataFrame,
      columnNames: Array[String],
      k: Int,
      suppressRatio: Double): Map[List[Any], Long] = {
    val suppLimit = src.count * suppressRatio
    var suppressedTuples = 0L

    val suppList = Map[List[Any], Long]()
    src.groupBy(columnNames.map(x => col(x)): _*).count
      .rdd.collect.map(group => {
        val nTuplesInGroup = group.getAs[Long](group.length-1)
        if(nTuplesInGroup < k) {
          suppressedTuples += nTuplesInGroup
          if(suppressedTuples >= suppLimit) return suppList
          else suppList += 
            (group.getValuesMap(columnNames).values.toList -> nTuplesInGroup)
        }
      })

    (suppList)
  }
  
  /**
   * Returns the suppressed row of given original row 
   *
   * @param row the row to be suppressed
   * @return Row new suppressed row
   */
  @deprecated("This function can not support different type of columns", "0.1")
  def makeSuppressedRow(
      row: Row): Row = {
    var rowList = Seq[Any]()
    for(col <- row.schema.fieldNames) 
      row.schema(col).dataType match {
        case n : NumericType => rowList = rowList :+ 0
        case _ => rowList = rowList :+ "*"
      }
    Row.fromSeq(rowList)
  }
  
  /**
   * Only changes the quasi-identifier columns of given row into '*',
   * and returns this changed row 
   *
   * @param row the row to be suppressed
   * @return Row new suppressed row
   */
  @deprecated("This function can not support different type of columns", "0.1")
  def makeSuppressedQuasiRow(
      row: Row,
      columnNames: Array[String]): Row = {
    var rowList = Seq[Any]()
    for(col <- row.schema.fieldNames)
      if(columnNames.contains(col)) {
        row.schema(col).dataType match {
          case n : NumericType => rowList = rowList :+ 0
          case _ => rowList = rowList :+ "*"
        }
      } else rowList = rowList :+ row.getAs(col)
      
    Row.fromSeq(rowList)
  }
  
}