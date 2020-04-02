package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.sql._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto.OrderbyFilterInfo
import ksb.csle.common.proto.StreamOperatorProto.OrderbyFilterInfo.Method
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that selects which rows in a given column of input dataframe should
 * be kept and which rows should be removed. Rows satisfying the given condition
 * are kept, remaining rows are removed.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.OrderbyFilterInfo]]
 *          OrderbyFilterInfo contains attributes as follows:
 *          - keyColName: Column which should be used for sorting
 *                        (required)
 *          - method: Direction of sorting (ascending, descending). Enum(ASC,
 *                    DESC) (required)
 *          - value: value to select rows (required)
 *
 *  ==OrderbyFilterInfo==
 *  {{{
 *  message OrderbyFilterInfo {
 *  required string keyColName = 1;
 *  required Method method = 2 [default = ASC];
 *  required int32 value = 3;  
 *  enum Method {
 *    ASC = 0;
 *    DESC = 1;
 *  }
 *  }
 *  }}}
 */
class OrderByFilterOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: OrderbyFilterInfo = o.getOrderbyFilter
  
  private def orderby(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : OrderByFilter")
    
    val column: Column = df.col(p.getKeyColName)
    val orderedDF = p.getMethod match {
      case Method.ASC => df.orderBy(column.asc)
      case Method.DESC => df.orderBy(column.desc)
      case _ => throw new RuntimeException("Unsupported method type.")
    }
    
    val result = orderedDF.limit(p.getValue)
    logger.debug(result.show.toString)
    logger.info(s"Row Count : ${result.count().toString()}")
    result
  }

  /**
   * Operates filtering function for data cleaning
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = orderby(df)
}

object OrderByFilterOperator {
  def apply(o: StreamOperatorInfo): OrderByFilterOperator = new OrderByFilterOperator(o)
}