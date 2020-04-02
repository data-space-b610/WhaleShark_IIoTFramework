package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions._

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that selects the columns that user wants to choose.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SelectColumnsInfo]]
 *          SelectColumnsInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be selected (repeated)
 *          - subParam: Option to select (repeated)
 *                      Ex: {"order","1000000300,1000000100,1000000200,1000000500"}
 *          - selectedColumnName: Column names to be selected (repeated)
 *
 *  ==SelectColumnsInfo==
 *  {{{
 *  message SelectColumnsInfo {
 *  repeated int32 selectedColumnId = 4;
 *  repeated SubParameter subParam = 5;
 *  repeated string selectedColumnName = 6;
 *  }
 *  }}}
 */
class ColumnSelectOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.SelectColumnsInfo =
    o.getSelectColumns

  private def selectColumns(df: DataFrame): DataFrame = {
    logger.debug(s"OpId ${o.getId} : SelectColumns")
    val columns1: Array[String] =
      Utils.getColumnNames(p.getSelectedColumnIdList, df)
    val columns2 = p.getSelectedColumnNameList.toList.toArray
    val columnNames = (columns1 ++ columns2).map(x => x.trim)
    val options = p.getSubParamList.map(x => x.getKey -> x.getValue).toMap
    val result =
      if (options.contains("order")) {
        var selectedCols: Array[String] = options.get("order").get.split(",")
          .map(x => x.trim())
        df.select(selectedCols.head, selectedCols.tail:_*)
      } else {
        df.select(columnNames.head, columnNames.tail:_*)
      }
     logger.debug(result.show.toString)
     logger.debug(result.printSchema.toString)

    result
  }

  /**
   * Operates selectColumns.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = selectColumns(df)
}

object ColumnSelectOperator {
  def apply(o: StreamOperatorInfo): ColumnSelectOperator =
    new ColumnSelectOperator(o)
}
