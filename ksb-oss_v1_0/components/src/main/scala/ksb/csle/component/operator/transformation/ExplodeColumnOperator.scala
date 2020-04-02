package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that creates a new row for each element in the given array or map column.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.ExplodeColumnInfo]]
 *          ExplodeColumnInfo contains attributes as follows:
 *          - srcColumnName: existing column name to explode (required)
 *          - destColumnName: output column name; if not set, will use 'srcColumnName' as output column name (optional)
 *
 * ==ExplodeColumnInfo==
 * {{{
 * message ExplodeColumnInfo {
 *  required string srcColumnName = 1;
 *  optional string destColumnName = 2;
 * }
 * }}}
 */
class ExplodeColumnOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getExplodeColumn == null) {
      throw new IllegalArgumentException("ExplodeColumnInfo is not set.")
    } else {
      o.getExplodeColumn
    }

  private[this] val srcColName = info.getSrcColumnName match {
    case null | "" =>
      throw new IllegalArgumentException("srcColumnName is not set.")
    case _ =>
      info.getSrcColumnName
  }

  private[this] val destColName = info.getDestColumnName match {
    case null | "" => info.getSrcColumnName
    case _ => info.getDestColumnName
  }

  /**
   * Operate creating a new row for each element in the given array or map column.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe that a new row is added.
   */
  override def operate(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    Try (df.schema(srcColName)) match {
      case Success(field) =>
        field.dataType match {
          case _: StructType | _: ArrayType =>
            df.withColumn(destColName, explode(df.col(srcColName)))
          case _ =>
            logger.info("can't explode '" + srcColName + "("
                + field.dataType.typeName + ")' column")
            df
        }
      case Failure(e) =>
        logger.info(s"no such column: '$srcColName'")
        df
    }
  }
}
