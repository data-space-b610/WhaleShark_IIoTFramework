package ksb.csle.component.pipe.stream.operator

import scala.collection.JavaConversions._

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.StreamPipeControlProto.StreamPipeOperatorInfo
import ksb.csle.common.proto.StreamPipeControlProto.OrderbyPipeInfo
import ksb.csle.common.proto.StreamPipeControlProto.OrderbyPipeInfo.Method
import ksb.csle.common.base.pipe.operator.BasePipeOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs to order by a specific column using a given condition.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamPipeControlProto.OrderbyPipeInfo]]
 *          OrderbyPipeInfo contains attributes as follows:
 *          - keyColName: Column which should be used for sorting (required)
 *          - method: Direction of sorting (ascending, descending). Enum(ASC,
 *                    DESC) (required)
 *
 *  ==OrderbyPipeInfo==
 *  {{{
 *  message OrderbyPipeInfo {
 *  required string keyColName = 1;
 *  required Method method = 2 [default = ASC];
 *  enum Method {
 *    ASC = 0;
 *    DESC = 1;
 *  }
 *  }
 *  }}}
 */
class OrderByOperator(
    o: StreamPipeOperatorInfo,
    session: SparkSession
    ) extends BasePipeOperator[DataFrame, StreamPipeOperatorInfo, SparkSession](
        o,
        session) {

  private val p: OrderbyPipeInfo = o.getOrderby
  private val keyColname: String = p.getKeyColName

 
  /**
   * Operates OrderByColumn.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: Any => DataFrame): DataFrame => DataFrame = df => {
    import session.implicits._
    
    val column = df.col(keyColname)
    p.getMethod match {
      case Method.ASC => df.orderBy(column.asc)
      case Method.DESC => df.orderBy(column.desc)
      case _ =>
        throw new RuntimeException("Unsupported method type.")
    }

  }
}
