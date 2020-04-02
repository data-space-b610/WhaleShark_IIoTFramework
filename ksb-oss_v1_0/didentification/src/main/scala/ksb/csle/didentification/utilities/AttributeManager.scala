package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import scala.collection.JavaConversions.asScalaBuffer

import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.DatasourceProto.FieldInfo

import ksb.csle.didentification.interfaces._

/**
 * This object manages the information about the attributes of columns.
 * It extracts some attribute information from given variables. 
 */
object AttributeManager {

  /**
   * Returns the name of identifier attribute columns among the given
   * 'columnInfos' variable
   * 
   * @param columnInfos the array of the information of columns
   * @return Array[String]
   */
  def getIdentifierColumnNames(columnInfos: java.util.List[FieldInfo]): Array[String] = 
    getIdentifierColumnNames((columnInfos map (_.asInstanceOf[FieldInfo])).toArray)
    
  def getIdentifierColumnNames(columnInfos: Array[FieldInfo]): Array[String] = 
    getSelectedTypeColumnNames(columnInfos, FieldInfo.AttrType.IDENTIFIER)
    
  /**
   * Returns the name of identifier attribute columns among the given
   * 'columnInfos' variable
   * 
   * @param hierarchies the map of the hierarchy information of columns
   * @return Array[String]
   */
  def getIdentifierColumnNames(
      hierarchies: Map[FieldInfo, ColumnHierarchy]): Array[String] = 
    getSelectedTypeColumnNames(hierarchies, FieldInfo.AttrType.IDENTIFIER)
    
  /**
   * Returns the name of quasi attribute columns among the given
   * 'columnInfos' variable
   * 
   * @param columnInfos the array of the information of columns
   * @return Array[String]
   */
  def getQuasiColumnNames(columnInfos: java.util.List[FieldInfo]): Array[String] = 
    getQuasiColumnNames((columnInfos map (_.asInstanceOf[FieldInfo])).toArray)
    
  def getQuasiColumnNames(columnInfos: Array[FieldInfo]): Array[String] = 
    getSelectedTypeColumnNames(columnInfos, FieldInfo.AttrType.QUASIIDENTIFIER)
    
  /**
   * Returns the name of quasi attribute columns by using the given
   * hierarchy information
   * 
   * @param hierarchies the map of the hierarchy information of columns
   * @return Array[String]
   */
  def getQuasiColumnNames(
      hierarchies: Map[FieldInfo, ColumnHierarchy]): Array[String] = 
    getSelectedTypeColumnNames(hierarchies, FieldInfo.AttrType.QUASIIDENTIFIER)
    
  def getSensColumnNames(columnInfos: java.util.List[FieldInfo]): Array[String] = 
    getSensColumnNames((columnInfos map (_.asInstanceOf[FieldInfo])).toArray)
    
  /**
   * Returns the name of sensitive attribute columns among the given
   * 'columnInfos' variable
   * 
   * @param columnInfos the array of the information of columns
   * @return Array[String]
   */
  def getSensColumnNames(columnInfos: Array[FieldInfo]): Array[String] = 
    getSelectedTypeColumnNames(columnInfos, FieldInfo.AttrType.SENSITIVE)
    

  /**
   * Returns the name of sensitive attribute columns by using the given
   * hierarchy information
   * 
   * @param hierarchies the map of the hierarchy information of columns
   * @return Array[String]
   */
  def getSensColumnNames(
      hierarchies: Map[FieldInfo, ColumnHierarchy]): Array[String] = 
    getSelectedTypeColumnNames(hierarchies, FieldInfo.AttrType.SENSITIVE)
    
  /**
   * Returns the name of columns specified the 'attr' attribute
   * 
   * @param columnInfos the array of the information of columns
   * @param attr the type of attribute
   * @return Array[String]
   */
  def getSelectedTypeColumnNames(
      columnInfos: Array[FieldInfo],
      attr: FieldInfo.AttrType): Array[String] = {
    var columnNames = Array[String]()
    columnInfos.map(col => {
      if(col.getAttrType == attr) columnNames = columnNames :+ col.getValue
    })
    (columnNames)
  }
    
  /**
   * Returns the name of columns specified the 'attr' attribute
   * 
   * @param hierarchies the map of the hierarchy information of columns
   * @param attr the type of attribute
   * @return Array[String]
   */
  def getSelectedTypeColumnNames(
      hierarchies: Map[FieldInfo, ColumnHierarchy],
      attr: FieldInfo.AttrType): Array[String] = {
    var columnNames = Array[String]()
    hierarchies.map(hierarhcy => {
      if(hierarhcy._1.getAttrType == attr)
        columnNames = columnNames :+ hierarhcy._1.getValue
    })
    
    (columnNames)
  }
  
}