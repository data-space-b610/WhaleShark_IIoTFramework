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
 *          [[ksb.csle.common.proto.StreamOperatorProto.SelectColumnsWithFileInfo]]
 *          SelectColumnsWithFileInfo contains attributes as follows:
 *          - columnIdPath: Path of file containing (required)
 *
 *  ==SelectColumnsInfo==
 *  {{{
 *  message SelectColumnsWithFileInfo {
 *    repeated int32 columnIdPath = 1;
 *  }
 *  }}}
 */
class ColumnSelectWithFileOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private val p: SelectColumnsWithFileInfo = o.getSelectColumnsWithFile
  private val path = p.getColumnIdPath
  private var columnNames: Array[String] = null

  private def selectColumns(df: DataFrame): DataFrame = {
    logger.debug(s"OpId ${o.getId} : SelectColumns")

    if (columnNames == null) {
      columnNames = df.sparkSession.sparkContext.textFile(path)
      .map(_.split(",")).collect.flatten
    }
    val result = df.select(columnNames.head, columnNames.tail:_*)

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

object ColumnSelectWithFileOperator {
  def apply(o: StreamOperatorInfo): ColumnSelectWithFileOperator =
    new ColumnSelectWithFileOperator(o)
}
