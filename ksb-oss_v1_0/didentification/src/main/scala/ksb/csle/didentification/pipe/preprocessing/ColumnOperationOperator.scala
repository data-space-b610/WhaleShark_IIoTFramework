package ksb.csle.didentification.pipe.preprocessing

import scala.collection.JavaConversions._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import ksb.csle.common.proto.StreamPipeControlProto.{StreamPipeOperatorInfo, ColOperationPipeInfo, ColumnOperationPipeInfo}
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

class ColumnOperationOperator(
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {
  
  val p: ColumnOperationPipeInfo = o.getColumOperations
  
  def columnOperation(df: DataFrame => DataFrame): DataFrame => DataFrame = 
    df => {
     logger.info(s"OpId ${o.getId} : ColumnOperationOperator")
     df.printSchema()
      val colOperationArray: Array[ColOperationPipeInfo] = 
        p.getOperationList.map(_.asInstanceOf[ColOperationPipeInfo]).toArray
        
      var result = df
      colOperationArray.map(data =>
        result = columnOperation(result, data))
     
      result
    }
  
  def columnOperation(src: DataFrame, colOperationInfo: ColOperationPipeInfo): DataFrame = {
    val columnIDs = (colOperationInfo.getColIdList map (_.asInstanceOf[Int])).toArray
    val columns = columnIDs.map(c => src.col(src.columns(c)))
//    val columns = src.columns.map(c => col(c)).toList.patch(0, Nil, 1)
    val columnName = colOperationInfo.getMadeColumnName
    colOperationInfo.getOperator match {
      case ColOperationPipeInfo.ColOperatorPipeType.COL_SUM => 
        src.withColumn(columnName, columns.reduce(_ + _))
      case ColOperationPipeInfo.ColOperatorPipeType.COL_MINUS => 
        src.withColumn(columnName, columns.reduce(_ - _))
      case ColOperationPipeInfo.ColOperatorPipeType.COL_MULTIPLY => 
        src.withColumn(columnName, columns.reduce(_ * _))
      case ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDE => 
        src.withColumn(columnName, columns.reduce(_ / _))
      case ColOperationPipeInfo.ColOperatorPipeType.COL_SUMVALUE => 
        sumValue(src, columnName, colOperationInfo.getValue)
      case ColOperationPipeInfo.ColOperatorPipeType.COL_MINUSVALUE => 
        minusValue(src, columnName, colOperationInfo.getValue)
      case ColOperationPipeInfo.ColOperatorPipeType.COL_MULTIPLYVALUE => 
        multiplyValue(src, columnName, colOperationInfo.getValue)
      case ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE => 
        divideValue(src, columnName, colOperationInfo.getValue)
    }    
  }
    
  private def sumValue(df: DataFrame, columnName: String, value: Double): DataFrame = {
    val sumValueUdf = udf[Double, String](_.toDouble + value)
    df.withColumn(columnName, sumValueUdf(df(columnName)))
  }
  
  private def minusValue(df: DataFrame, columnName: String, value: Double): DataFrame = {
    val minusValueUdf = udf[Double, String](_.toDouble - value)
    df.withColumn(columnName, minusValueUdf(df(columnName)))
  }
  
  private def multiplyValue(df: DataFrame, columnName: String, value: Double): DataFrame = {
    val multiplyValueUdf = udf[Double, String](_.toDouble * value)
    df.withColumn(columnName, multiplyValueUdf(df(columnName)))
  }
  
  private def divideValue(df: DataFrame, columnName: String, value: Double): DataFrame = {
    val divideValueUdf = udf[Double, String](_.toDouble / value)
    df.withColumn(columnName, divideValueUdf(df(columnName)))
  }
  
  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = columnOperation(df)
  
}
