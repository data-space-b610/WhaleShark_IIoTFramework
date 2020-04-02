package ksb.csle.component.operator.util

import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto._

/**
 * Utils object to use in data preprocessing classes.
 */
object Utils {
  val DEBUG: Boolean = true
  val SHOW_NUM: Int = 6

  /**
   * Gets multiple column names using following params.
   * 
   * @param columnIdList The column ids list to obtain the column names.
   * @param src Input dataframe.
   * @return Array[String] The array string of column names.
   */ 
  def getColumnNames(
      columnIdList: java.util.List[Integer],
      src: Dataset[Row]): Array[String] = {
    val columnIdArray: Array[Int] = 
      columnIdList.toArray.map(i => i.asInstanceOf[Int])
    val columnNames: Array[String] = columnIdArray.map(x => src.columns(x))
    (columnNames)
  }

  /**
   * Gets single column name using following params.
   * 
   * @param columnId The column id to obtain the column name.
   * @param src Input dataframe.
   * @return String The string of column name.
   */ 
  def getSingleColumnName(columnId : Integer, src: Dataset[Row]): String = {
    return src.columns(columnId)
  }

  /**
   * Prints debug information using following params.
   * 
   * @param data Input dataframe.
   * @param num The number of rows to be shown.
   * @return Unit.
   */ 
  def printDEBUG(data: Dataset[Row], num: Int): Unit = {
    if(DEBUG) {
      data.show(num)
//      data.printSchema
    }
  }

  def printNColumns(data: Dataset[Row], num: Int): Dataset[Row] = {
    val column_names_str = data.schema.map(f => s"${f.name}").take(num)
    val column_names_col = column_names_str.map(name => col(name)) 
    val data_new = data.select(column_names_col:_*)
    data_new
  }
  /**
   * Prints debug information using following params.
   * It shows 6 rows by default.
   * 
   * @param data Input dataframe.
   * @return Unit.
   */
  def printDEBUG(data: Dataset[Row]): Unit = {
    printDEBUG(data, SHOW_NUM)
  }

  /**
   * Merges files using following params.
   * 
   * @param srcPath The file source path.
   * @param dstPath The file destination path. 
   * @return Unit.
   */
  def mergeFile(srcPath: String, dstPath: String): Unit = {
    scala.reflect.io.Path.string2path(dstPath).delete()
    val srcDir = new org.apache.hadoop.fs.Path(srcPath)
    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = srcDir.getFileSystem(conf)
    val dstFile = new org.apache.hadoop.fs.Path(dstPath)
    org.apache.hadoop.fs.FileUtil.copyMerge(fs, srcDir, fs, dstFile, false, 
      conf, null)
  }

  /**
   * Checkes aggregate methods using following params.
   * 
   * @param method The aggregate method to be checked.
   * @return String The result string of method to be checked.
   */
  def checkAggregateMethods(method: String): String = {
    // reference: http://spark.apache.org/docs/latest/api/scala/index.html#org.
    // apache.spark.sql.RelationalGroupedDataset
    // 2016.11.24: avg, count, max, mean, min, sum
    val allowedFunction: Seq[String] = 
      ("avg")::("count")::("max")::("mean")::("min")::("sum")::Nil
    return if(allowedFunction.contains(method)) method else "avg"
  }
}
