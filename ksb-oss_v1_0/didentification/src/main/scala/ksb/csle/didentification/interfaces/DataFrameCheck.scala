package ksb.csle.didentification.interfaces

import org.apache.spark.sql.DataFrame
import ksb.csle.common.proto.DatasourceProto.FieldInfo

/**
 * This trait provides some functions to get specific columns or
 * check the given column names or IDs of dataframe are valid or not.
 */
trait DataFrameCheck {
  
  def getQuasiColumnIDs(fieldInfos: Array[FieldInfo]): Array[Int] =
    fieldInfos.filter(_.getAttrType ==
        FieldInfo.AttrType.QUASIIDENTIFIER).map(_.getKey.toInt)

  def getSensColumnIDs(fieldInfos: Array[FieldInfo]): Array[Int] =
    fieldInfos.filter(_.getAttrType ==
        FieldInfo.AttrType.SENSITIVE).map(_.getKey.toInt)
              
  /**
   * Returns column names from src dataframe specified by column IDs.
   * Note that the column with invalid IDs are ignored.
   *
   * @param src dataframe to get names of columns.
   * @param columnId the array of column IDs to anonymize.
   * @return Array[String].
   */
  def getColumnNames(src: DataFrame, columnIDs: Array[Int]): Array[String] =
    getValidColumnNames(src, columnIDs)
        
  private def getValidColumnNames(
      src: DataFrame, columnIDs: Array[Int]): Array[String] = 
    getValidColumnIDs(src, columnIDs) map (getColumnName(src, _))
  
  def getValidColumnIDs(
      src: DataFrame, columnIDs: Array[Int]): Array[Int] = 
    columnIDs.filter(isValidColumnID(src, _))
  
  /**
   * Returns column name from src dataframe
   * specified by the column ID defined by protobuf.
   *
   * @param src dataframe to get names of columns.
   * @param columnId column ID to anonymize.
   * @return String.
   */
  def getColumnName(src: DataFrame, columnId: Int): String = {
    require(isValidColumnID(src, columnId), "The column ID is not valid")
    src.columns(columnId)
  }

  /**
   * Checks the given column Name is valid.
   *
   * @param src dataframe to get names of columns.
   * @param columnName column Name.
   * @return Boolean.
   */
  def isValidColumnName(src: DataFrame, columnName: String): Boolean = 
    src.columns.contains(columnName)

  /**
   * Checks the given column ID is valid.
   *
   * @param src dataframe to get names of columns.
   * @param columnId column ID.
   * @return Boolean.
   */
  def isValidColumnID(src: DataFrame, columnID: Int): Boolean = 
    (0 <= columnID && columnID < src.columns.length)
    
}