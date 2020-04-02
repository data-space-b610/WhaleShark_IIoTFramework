package ksb.csle.component.operator.integration

import com.google.protobuf.Message
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto.GroupbyInfo
import ksb.csle.common.proto.StreamOperatorProto.GroupbyInfo.GroupbyOp
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that creates dataframe by aggregating records
 * according to a given group condition.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.GroupbyInfo]]
 *          GroupbyInfo contains attributes as follows:
 *          - keyColName: Column name to be used as a key column (required)
 *          - valColName: Column name to be used as a value column (required)
 *          - groupby:    method for aggregation. Enum(SUM, COUNT), (required)
 *
 *  ==GroupbyInfo==
 *  {{{
 *  message GroupbyInfo {
 *  required string keyColName = 1;
 *  required string valColName = 2;
 *  enum GroupbyOp {
 *       SUM = 0;
 *       COUNT = 1;
 *      }
 *      required GroupbyOp groupby = 3;
 *  }
 *  }}}
 */
class GroupByOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o){

  val p: GroupbyInfo = o.getGroupby

  private def groupby(df: DataFrame): DataFrame = {
    logger.info(s"OpId: ${o.getId}, ClassName: ${o.getClsName}")
    logger.debug(s"OpId: ${o.getId}, ClassName: ${o.getClsName}")

    p.getGroupby match {
      case GroupbyOp.SUM =>
        (df.groupBy(p.getKeyColName).sum(p.getValColName))
      case GroupbyOp.COUNT => (df.groupBy(p.getKeyColName).count())
      case _ =>
        logger.warn("Unsupported Groupby type.")
        df
    }
  }
  /**
   * Operates GroupBy.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = groupby(df)
}

object GroupByOperator {
  def apply(o: StreamOperatorInfo): GroupByOperator = new GroupByOperator(o)
}
