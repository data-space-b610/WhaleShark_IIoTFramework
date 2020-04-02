package ksb.csle.component.pipe.stream.operator

import scala.collection.JavaConversions._

import com.google.protobuf.Message

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.base.pipe.operator.BasePipeOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.DefaultResult
//import ksb.csle.data.util._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that selects the columns that user wants to choose.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SelectColumnsPipeInfo]]
 *          SelectColumnsPipeInfo contains attributes as follows:
 *          - col: Column ids to be selected (repeated)
 *          - colName: Column names to be selected (repeated)
 *          - isRename: set "true" if you want to change the column names 
 *                      to be selected (optional)
 *          - newName: Column names to be changed (repeated)
 *
 *  ==SelectColumnsInfo==
 *  {{{
 *  message SelectColumnsPipeInfo {
 *  repeated int32 col = 1;
 *  repeated string colName = 2;
 *  optional bool isRename = 3 [default = false];
 *  repeated string newName = 4;
 *  }
 *  }}}
 */
class SelectColumnsPipeOperator(
    o: StreamPipeOperatorInfo,
    session: SparkSession
    ) extends BasePipeOperator[DataFrame, StreamPipeOperatorInfo, SparkSession](
        o,
        session) {

  val p: SelectColumnsPipeInfo = o.getSelectColumns
  val selectedCols: Array[String] = p.getColNameList.map(col => col.toString()).toArray
  val isRename = p.getIsRename
  val newCols = p.getNewNameList

  private def selectColumns(df: DataFrame): DataFrame = {
    val newdf = df.select(selectedCols.head, selectedCols.tail:_*)
    
    if (isRename) 
      newdf.toDF(newCols: _*) 
    else
      newdf

  }

  /**
   * Selects specific columns.
   *
   * @param  df Input function pipe
   * @return output data pipe
   */
  override def operate(df: Any => DataFrame) = df => selectColumns(df)
}

object SelectColumns {
  def apply(
      o: StreamPipeOperatorInfo,
      s: SparkSession) = new SelectColumnsPipeOperator(o, s)
}
