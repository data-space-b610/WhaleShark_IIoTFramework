package ksb.csle.component.operator.integration

import com.google.protobuf.Message
import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.component.operator.util._
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.proto.StreamOperatorProto.OrderByColumnInfo.Method
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs to order by a specific column using a given condition.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.OrderByColumnInfo]]
 *          OrderByColumnInfo contains attributes as follows:
 *          - selectedColumnId: Column which should be used for sorting (required)
 *          - method: Direction of sorting (ascending, descending). Enum(ASC,
 *                    DESC) (required)
 *
 *  ==OrderByColumnInfo==
 *  {{{
 *  message OrderByColumnInfo {
 *  required int32 selectedColumnId = 4;
 *  required Method method = 5 [default = ASC];
 *  enum Method {
 *    ASC = 0;
 *    DESC = 1;
 *  }
 *  }
 *  }}}
 */
class OrderByOperator(
    o: StreamOperatorInfo) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o){

  val p: ksb.csle.common.proto.StreamOperatorProto.OrderByColumnInfo = o.getOrderByColumn

  private def orderByColumn(df: DataFrame): DataFrame = {
    logger.debug(s"OpId ${o.getId} : OrderByColumn")
    val selectedColumnName: String =
      Utils.getSingleColumnName(p.getSelectedColumnId, df)
    val column: Column = df.col(selectedColumnName)
    val result = p.getMethod match {
      case Method.ASC => df.orderBy(column.asc)
      case Method.DESC => df.orderBy(column.desc)
      case _ =>
        throw new RuntimeException("Unsupported method type.")
    }
    logger.debug(result.show.toString)
    logger.debug(result.printSchema.toString)
    result
  }

  /**
   * Operates OrderByColumn.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = orderByColumn(df)
}

object OrderByOperator {
  def apply(o: StreamOperatorInfo): OrderByOperator = new OrderByOperator(o)
}