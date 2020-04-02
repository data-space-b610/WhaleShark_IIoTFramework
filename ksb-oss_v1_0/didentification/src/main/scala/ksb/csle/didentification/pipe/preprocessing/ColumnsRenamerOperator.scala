package ksb.csle.didentification.pipe.preprocessing

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{ SparkSession, DataFrame }

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import ksb.csle.common.proto.StreamPipeControlProto.StreamPipeOperatorInfo
import ksb.csle.common.proto.StreamPipeControlProto.{RenameColumn, RenameColumnsInfo}
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

import ksb.csle.didentification.utilities.FileManager

class ColumnsRenamerOperator(
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {

  val p: RenameColumnsInfo = o.getRenameColumns
  // the index should start at 1
  private val columnNamesMap = 
    if(p.hasColNameFromPath()) {
      val colNamesDF = FileManager.makeDataFrameURL(s,p.getColNameFromPath,true,",")
      colNamesDF.collect.map(row =>
        (row.getAs[String](0).toInt-1 -> row.getAs[String](1).trim()))
    } else {
      p.getColumnsList.map(_.asInstanceOf[RenameColumn]).toArray.map(col =>
        (col.getColIndex-1 -> col.getColName.trim()))
    }

  def renameColumns(df: DataFrame => DataFrame): DataFrame => DataFrame =
    df => {
      logger.info(s"OpId ${o.getId} : Rename Columns Info")
      if(p.hasColNameFromPath()) {
        df.toDF(columnNamesMap.map(_._2) :_*)
      } else {
        var result = df
        columnNamesMap.map(x =>
          result = result.withColumnRenamed(df.columns(x._1), x._2))
        (result)
      }
    }
  
  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = renameColumns(df)
}
