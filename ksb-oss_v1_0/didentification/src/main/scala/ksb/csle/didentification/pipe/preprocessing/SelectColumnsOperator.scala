package ksb.csle.didentification.pipe.preprocessing

import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{ SparkSession, DataFrame }

import ksb.csle.common.proto.StreamPipeControlProto.{StreamPipeOperatorInfo, SelectColumnsPipeInfo}
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

class SelectColumnsOperator(
    o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {
  
  val p: SelectColumnsPipeInfo = o.getSelectColumns

  def selectColumns(df: DataFrame => DataFrame): DataFrame => DataFrame =
    df => {
      df.printSchema()
      logger.info(s"OpId ${o.getId} : SelectColumnsPipe")
      val columns1: Array[String] =
        p.getColList.toList.toArray.map(x => df.columns(x.toInt))
      val columns2 = p.getColNameList.toList.toArray
      val columnNames = (columns1 ++ columns2).map(x => x.trim)
      df.select(columnNames.head, columnNames.tail:_*)
    }
  
  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = selectColumns(df)
}
