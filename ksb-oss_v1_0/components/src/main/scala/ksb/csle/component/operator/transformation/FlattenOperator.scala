package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vectors, Vector, DenseVector}

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that flattens column.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.FlattenInfo]]
 *          FlattenInfo contains attributes as follows:
 *          - columnName: column name (required)
 *
 * ==FlattenInfo==
 * {{{
 * message FlattenInfo {
 *   required string columnName = 1;
 * }
 * }}}
 */
class FlattenOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getFlatten == null) {
      throw new IllegalArgumentException("FlattenInfo is not set.")
    } else {
      o.getFlatten
    }

  /**
   * Operate flattening column.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe which are flattened.
   */
  override def operate(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    Try {
      val field = df.schema.apply(info.getColumnName)
      val targetCol = info.getColumnName
      val groupByCol = "_gbkey_"
      val groupId = 1

      val collected = df
        .withColumn(groupByCol, lit(groupId))
        .groupBy(groupByCol)
        .agg(collect_list(targetCol) as targetCol)

      collected.map { row =>
        val vectorArr = row.getSeq[Vector](1)
        vectorArr.map(_.toArray).flatten
      }.toDF(targetCol)
    } match {
      case Success(flattened) =>
        flattened
      case Failure(e) =>
        logger.warn("can't flatten DataFrame: " + e.getMessage)
        df
    }
  }
}
