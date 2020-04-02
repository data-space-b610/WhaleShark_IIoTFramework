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
 * Operator that changes the column names to be selected. 
 * In addition, changes the column type to be selected. 
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamPipeControlProto.RenameColumnsPipeInfo]]
 *          RenameColumnsPipeInfo contains attributes as follows:
 *          - selectedColumn: Column information to be selected (repeated)
 *
 *  ==RenameColumnsPipeInfo==
 *  {{{
 *  message RenameColumnsPipeInfo {
 *  repeated SelectedColumnInfo selectedColumn = 1;
 *  }
 *  }}}
 *
 *  ==SelectedColumnInfo==
 *  {{{
 *  message SelectedColumnInfo {
 *  required int32 selectedColIndex = 1;
 *  required string newColName = 2;
 *  optional FieldType newFieldType = 3 [default = STRING];
 * }
 * }}}
 */
class RenameColumnsPipeOperator(
    o: StreamPipeOperatorInfo,
    session: SparkSession
    ) extends BasePipeOperator[DataFrame, StreamPipeOperatorInfo, SparkSession](
        o,
        session) {

  val p: RenameColumnsPipeInfo = o.getRenameCol
  val selectedCols = p.getSelectedColumnList.map(_.asInstanceOf[SelectedColumnInfo]).toArray
    .map{ x => 
      if(x.hasNewFieldType()) {
        (x.getSelectedColIndex, x.getNewColName.trim(), x.getNewFieldType.toString())
      }
      else {
        (x.getSelectedColIndex, x.getNewColName.trim(), "")
      }
    }

  private def renameColumns(df: DataFrame): DataFrame = {
    val colNames = df.columns
    var result = df
    selectedCols.map{ x =>
      if (x._3.isEmpty()) {
        result = result.withColumnRenamed(df.columns(x._1), x._2)
      }
      else {
        result = result.withColumn(x._2, df.col(df.columns(x._1)).cast(x._3))
      }
      colNames(x._1) = x._2
    }
    result.select(colNames.head, colNames.tail:_*)
  }

  /**
   * Changes specific column name and type.
   *
   * @param  df Input function pipe
   * @return output data pipe
   */
  override def operate(df: Any => DataFrame) = df => renameColumns(df)
}

object RenameColumns {
  def apply(
      o: StreamPipeOperatorInfo,
      s: SparkSession) = new RenameColumnsPipeOperator(o, s)
}
