package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification._
import ksb.csle.didentification.interfaces._

/**
 * This object manages hierarchical information of column attributes
 */
object HierarchyManager {
  
  def getQuasiHierarchies(
      data: DataFrame, 
      columnInfos: Array[FieldInfo]) 
  : Map[FieldInfo, ColumnHierarchy] = {
    val fileConfiguredColumns = 
      columnInfos.filterNot(_.getAutoConfigured)
      
    val autoConfigureColumns = 
      columnInfos.filter(_.getAutoConfigured)
      
    getQuasiHierarchiesFromFiles(data.sparkSession, fileConfiguredColumns)
      .++:(getAutomaticHierarchies(data, autoConfigureColumns,
          FieldInfo.AttrType.QUASIIDENTIFIER))
  }
  
  def getIdenHierarchiesFromFiles(
      spark: SparkSession, 
      columnInfos: Array[FieldInfo]) 
  : Map[FieldInfo, ColumnHierarchy] =
    confHierarchiesFromFiles(spark, columnInfos, FieldInfo.AttrType.IDENTIFIER)
    
  /**
   * The member variable columnInfos contains various information of columns
   * such as the column ID, name, the path of file including hierarchical
   * information. This function returns the information of quasi-identifier 
   * columns [info. about column, its column hierarchy] by using columnInfos.
   * 
   * @param spark Spark Session
   * @param columnInfos the detailed information of column
   * @return Map[FieldInfo, ColumnHierarchy]
   */
  def getQuasiHierarchiesFromFiles(
      spark: SparkSession, 
      columnInfos: Array[FieldInfo]) 
  : Map[FieldInfo, ColumnHierarchy] =
    confHierarchiesFromFiles(spark, columnInfos, FieldInfo.AttrType.QUASIIDENTIFIER)
    
  /**
   * Returns the map information of sensitive columns
   * [info. about column, its column hierarchy] by using columnInfos
   * 
   * @param spark Spark Session
   * @param columnInfos the detailed information of column
   * @return Map[FieldInfo, ColumnHierarchy]
   */
  def getSensitiveHierarchiesFromFiles(
      spark: SparkSession, 
      columnInfos: Array[FieldInfo]) 
  : Map[FieldInfo, ColumnHierarchy] =
    confHierarchiesFromFiles(spark, columnInfos, FieldInfo.AttrType.SENSITIVE)

  /**
   * Returns the map information of columns [info. of column, its hierarchy]
   * specified 'attr' attribute by using columnInfos
   * 
   * @param spark Spark Session
   * @param columnInfos the detailed information of column
   * @param attr attribute types (e.x., identifier, quasi-identifier, 
   * sensitive, non-sensitive attributes)
   * @return Map[FieldInfo, ColumnHierarchy]
   */
  def confHierarchiesFromFiles(
      spark: SparkSession, 
      columnInfos: Array[FieldInfo],
      attr: FieldInfo.AttrType)
  : Map[FieldInfo, ColumnHierarchy] = {
    val hierarchies = Map[FieldInfo, ColumnHierarchy]()
    columnInfos.map(col => 
      if(col.getAttrType == attr)  
        hierarchies += (col -> {
          val hierarchyDF = FileManager.makeDataFrameURL(
              spark, col.getFilePath)
          getColumnHierarchyFromDataFrame(hierarchyDF)
        })
    )

    (hierarchies)
  }
  
  /**
   * Extracts the hierarhcy information from the file containing hierarhcy.
   * Note that the file is consisted of [original data of a column, the  
   * data by generalizing one times, the data by generalizing second times,
   * ..., the data by generalizing n times]. This function makes the column
   * hierarchy by using the read dataframe (hierarchy)
   * 
   * @param hierarchy the DataFrame storing the hierarchy information
   * @return ColumnHierarchy
   */
  def getColumnHierarchyFromDataFrame(
      hierarchy: DataFrame): ColumnHierarchy = {
    val hierarhcyTables = Map[Int, HierarchyEntries]()
    for(depth <- 1 until hierarchy.columns.length) {
      val generalizedTuples = Map[String, String]()
      val tmpDropDuplicated = hierarchy.select(
          hierarchy.columns(0), hierarchy.columns(depth))
          .dropDuplicates
      tmpDropDuplicated.rdd.collect.map(row => 
        generalizedTuples += (row.getAs[String](0) -> row.getAs[String](1)))
      
      hierarhcyTables += (depth -> 
        new HierarchyEntries(depth, generalizedTuples))
    }

    new ColumnHierarchy(hierarhcyTables)
  }
  
  /**
   * Automatically configures the hierarchy information from the given
   * 'src' dataframe where the column type is equal to the given 'attr'
   * attribute. The depth of hierarchy is nLevels.
   * 
   * @param src The dataframe to configure hierarchy
   * @param columnInfos The array of field info which contains the meta
   * information of the dataframe
   * @param nLevels The desired depth of hierarchy to configure
   * @param attr The attribute types (e.x., identifier, quasi-identifier, 
   * sensitive, non-sensitive attributes)
   * @return Map[FieldInfo, ColumnHierarchy]
   */
  def getAutomaticHierarchies(
      src: DataFrame,
      columnInfos: Array[FieldInfo],
      attr: FieldInfo.AttrType)
  : Map[FieldInfo, ColumnHierarchy] = {
    val hierarchies = Map[FieldInfo, ColumnHierarchy]()
    columnInfos.map(col => 
      if(col.getAttrType == attr)  
        hierarchies += (col -> {
          val nLevels = col.getNLevels 
          val colName = src.columns(col.getKey.toInt)
          var df = src.select(colName)
          for(i <- nLevels to 1 by -1) {
            val generalizedSrc = GeneralizeManager.generalizing(src, colName, i)
            df = MergingDataframe.merging(df, generalizedSrc
                .select(colName).withColumnRenamed(colName, colName+i))
          }
          getColumnHierarchyFromDataFrame(df)
        })
    )

    (hierarchies)
  }
    
}