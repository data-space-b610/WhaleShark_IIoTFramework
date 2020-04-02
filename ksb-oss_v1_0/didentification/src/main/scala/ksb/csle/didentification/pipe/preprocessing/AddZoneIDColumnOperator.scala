package ksb.csle.didentification.pipe.preprocessing

import scala.collection.JavaConversions._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.base.pipe.operator.BaseSimplePipeOperator

/**
 * This class implements the column manipulation module which manipulates the 
 * contents type
 */
class AddZoneIDColumnOperator(o: StreamPipeOperatorInfo,
    s: SparkSession) extends BaseSimplePipeOperator[
      DataFrame, StreamPipeOperatorInfo, SparkSession](o, s) {
  
  val p: AddZoneIDPipeColumnInfo = o.getAddZoneId

  def addZoneIDColumn(df: DataFrame => DataFrame): DataFrame => DataFrame = 
    df => {
      logger.info(s"OpId ${o.getId} : AddZoneIDColumn")
    
      var sensorId = 0
      val makeSensorColum: (String => Int) = key => {
        p.getInfoType match {
          case ZoneIDPipeInfoType.ZONEID_PIPE_INCREMENTAL => {
            sensorId = sensorId + 1
            (sensorId - 1)
          }
          case ZoneIDPipeInfoType.ZONEID_PIPE_FIXED => p.getZoneID
          case ZoneIDPipeInfoType.ZONEID_PIPE_UNIFORM => {
            val min = p.getUniformInfo.getMin
            val max = p.getUniformInfo.getMax
            min + scala.util.Random.nextInt( (max - min) + 1 )  
          }
        }
      }
      val sensorColumnUdf = udf(makeSensorColum)
      df.withColumn(p.getColumnName, sensorColumnUdf(df.col(df.columns(0))))
    }
  
  override def operate(df: DataFrame => DataFrame): DataFrame => DataFrame = addZoneIDColumn(df)
  
}
