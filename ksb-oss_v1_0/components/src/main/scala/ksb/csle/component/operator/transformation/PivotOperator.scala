package ksb.csle.component.operator.transformation

import scala.collection.JavaConversions._

import com.google.protobuf.Message
import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.PivotInfo
import ksb.csle.common.proto.StreamOperatorProto.PivotInfo.Method
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that rotates the given dataframe by grouping multiple rows of same
 * groups to single rows.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.PivotInfo]]
 *          PivotInfo contains attributes as follows:
 *          - selectedColumnName: Column id which should be used for pivoting
 *                               (required)
 *          - groupByColumn: Column set to do groupby (required)
 *          - valueColumn: Column to be used as a value (required)
 *          - method: Method of aggregation to groupby. Enum(AVG, SUM) (required)
 *
 *  ==PivotInfo==
 *  {{{
 *  message PivotInfo {
 *  required int32 selectedColumnId = 4;
 *  required string groupByColumn = 5;
 *  required string valueColumn = 6;
 *  required Method method = 7 [default = AVG];
 *  enum Method {
 *    AVG = 0;
 *    SUM = 1;
 *  }
 *  }
 *  }}}
 */
class PivotOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o){

  val p: ksb.csle.common.proto.StreamOperatorProto.PivotInfo = o.getPivot

  private def pivot(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : Pivot")
    val selectedColumnName: String =
      Utils.getSingleColumnName(p.getSelectedColumnId, df)
    val groupByCols: Array[String] =
      p.getGroupByColumn.split(",").map(x => df.columns(x.trim.toInt))
    val valueColumnName: String =
      df.columns(p.getValueColumn.toInt)
    val agg_method: String = p.getMethod match {
      case Method.AVG => "avg"
      case Method.SUM => "sum"
      case _   => "avg"
    }
    val agg_method_checked = Utils.checkAggregateMethods(agg_method)
    val groupCols: Array[Column] = groupByCols.map(x => df.col(x))
    val result = df.groupBy(groupCols:_*)
      .pivot(selectedColumnName)
      .agg(ImmutableMap.of(valueColumnName, agg_method_checked))
      .sort(groupCols:_*)
    val selectedDf = Utils.printNColumns(result,10)
    logger.debug("Output dataframe :" + selectedDf.show.toString)
    result
  }

  /**
   * Operates Pivot.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = pivot(df)
}
object PivotOperator {
  def apply(o: StreamOperatorInfo): PivotOperator = new PivotOperator(o)
}