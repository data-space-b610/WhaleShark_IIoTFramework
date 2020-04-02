package ksb.csle.didentification.utilities

import scala.io.Source
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ksb.csle.didentification.exceptions.IOExceptionHandler

/**
 * This object provides the function to make a dataframe from the file info.
 * The file location can be given by the form of directory or URL.
 */
object FileManager {
  
  /**
   * Makes a dataframe by reading the file. The path of its file
   * is given by the format of directory.
   * 
   * @param spark Spark Session
   * @param path The path of file
   * @param hasHeader Checks whether the header exists
   * @param delimiter The column separator
   * @param schema The schema info of a given file
   * @return DataFrame
   */
  def makeDataFrame(spark: SparkSession, path: String)
    : DataFrame = makeDataFrame(spark, path, false)
    
  def makeDataFrame(
      spark: SparkSession, path: String, hasHeader: Boolean)
    : DataFrame = makeDataFrame(spark, path, hasHeader, ";")
    
  def makeDataFrame(
      spark: SparkSession, 
      path: String,
      hasHeader: Boolean,
      delimiter: String): DataFrame = {
    println("PATH: " + path)
    val result = IOExceptionHandler(
        Source.fromFile(path)) { file =>
          spark.read.format("csv")
          .option("sep", delimiter)
          .option("header", hasHeader)
          .option("inferSchema", "true")
          .load(path)
          .cache
    }
    
    result.asInstanceOf[DataFrame]
  }
  
  def makeDataFrame(
      spark: SparkSession, 
      path: String,
      hasHeader: Boolean,
      delimiter: String,
      schema: StructType): DataFrame = {
    val result = IOExceptionHandler(
        Source.fromFile(path)) { file =>
          spark.read.format("csv")
          .option("sep", delimiter)
          .option("header", hasHeader)
          .schema(schema)
          .load(path)
          .cache
    }
    
    result.asInstanceOf[DataFrame]
  }
  
  /**
   * Makes a dataframe by reading the file
   * 
   * @param spark Spark Session
   * @param path The path of file
   * @param hasHeader Checks whether the header exists
   * @param delimiter The column separator
   * @param schema The schema info of a given file
   * @return DataFrame
   */
  def makeDataFrameURL(spark: SparkSession, path: String)
    : DataFrame = makeDataFrameURL(spark, path, false)
    
  def makeDataFrameURL(
      spark: SparkSession, path: String, hasHeader: Boolean)
    : DataFrame = makeDataFrameURL(spark, path, hasHeader, ";")
    
  def makeDataFrameURL(
      spark: SparkSession, 
      path: String,
      hasHeader: Boolean,
      delimiter: String): DataFrame = {
    val result = IOExceptionHandler(
        Source.fromURL(path)) { file =>
          spark.read.format("csv")
          .option("sep", delimiter)
          .option("header", hasHeader)
          .load(path)
          .cache
    }
    
    result.asInstanceOf[DataFrame]
  }
  
  def makeDataFrameHDFS(
      spark: SparkSession, 
      path: String,
      hasHeader: Boolean,
      delimiter: String): DataFrame = {
    val result = spark.read.format("csv")
        .option("sep", delimiter)
        .option("header", hasHeader)
        .option("inferSchema", "True")
        .load(path)
        .cache
    
    result.asInstanceOf[DataFrame]
  }
    
}