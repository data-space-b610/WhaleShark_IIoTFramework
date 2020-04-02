package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.FilterInfo.Condition
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that removes the selected columns in the given dataframe.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.RemoveSelectedColumnsInfo]]
 *          RemoveSelectedColumnsInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be removed. (repeated)
 *
 *  ==RemoveSelectedColumnsInfo==
 *  {{{
 *  message RemoveSelectedColumnsInfo {
 *  repeated int32 selectedColumnId = 4;
 *  }
 *  }}}
 */
class ColumnRemoveOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.RemoveSelectedColumnsInfo =
    o.getRemoveCol

  private def removeSelectedColumns(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : RemoveSelectedColumns")
//    logger.info("Input dataframe :" + df.show.toString)
    val columnNames: Array[String] =
      Utils.getColumnNames(p.getSelectedColumnIdList, df)
    var result = df.drop(columnNames:_*)
    val selectedDf = Utils.printNColumns(result,10)
    logger.info("Output dataframe :" + selectedDf.show.toString)
//    logger.info("output dataframe :" + result.show.toString)
//    logger.info(result.printSchema.toString)
    result
  }

  /**
   * Operates removeSelectedColumns function using following params.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = removeSelectedColumns(df)
}

object ColumnRemoveOperator {
  def apply(o: StreamOperatorInfo): ColumnRemoveOperator =
    new ColumnRemoveOperator(o)
}