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
 * Operator that splits the existing column into multiple columns.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SplitColumnInfo]]
 *          SplitColumnInfo contains attributes as follows:
 *          - srcColumnName: existing column name to explode (required)
 *          - keepSrcColumn: whether keeps the source column; default is false (optional)
 *
 * ==SplitColumnInfo==
 * {{{
 * message SplitColumnInfo {
 *   required string srcColumnName = 1;
 *   optional bool keepSrcColumn = 2 [default = false];
 * }
 * }}}
 */
class SplitColumnOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getSplitColumn == null) {
      throw new IllegalArgumentException("SplitColumnInfo is not set.")
    } else {
      o.getSplitColumn
    }

  private[this] val srcColName = info.getSrcColumnName match {
    case null | "" =>
      throw new IllegalArgumentException("srcColumnName is not set.")
    case _ =>
      info.getSrcColumnName
  }

  /**
   * Operate splitting the existing column into multiple columns.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has splitted columns.
   */
  override def operate(df: DataFrame): DataFrame = {
    Try (df.schema(srcColName)) match {
      case Success(field) =>
        val splitted = split(df, field, srcColName)
        if (info.getKeepSrcColumn) {
          splitted
        } else {
          splitted.drop(srcColName)
        }
      case Failure(e) =>
        logger.info(s"no such column: '$srcColName'")
        df
    }
  }

  private def split(df: DataFrame, field: StructField,
      name: String): DataFrame = {
    import df.sparkSession.implicits._

    field.dataType match {
      case stType: StructType =>
        val exprs = stType.fields.array.map { f =>
          (f.name, col(s"$name.${f.name}"))
        }
        exprs.foldLeft(df) { case (src, (name, expr)) =>
          src.withColumn(name, expr)
        }
      case arrType: ArrayType =>
        val elementCount = df.first().getAs[Seq[_]](name).length
        val exprs = 0 until elementCount map { idx =>
          (s"$name$idx", col(name).getItem(idx))
        }
        exprs.foldLeft(df) { case (src, (name, expr)) =>
          src.withColumn(name, expr)
        }
      case _ =>
        logger.info("can't split '" + srcColName + "("
            + field.dataType.typeName + ")' column")
        df
    }
  }
}
