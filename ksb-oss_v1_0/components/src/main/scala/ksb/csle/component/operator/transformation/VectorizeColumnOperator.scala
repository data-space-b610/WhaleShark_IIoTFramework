package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that vectorizes multiple columns to one column.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.VectorizeColumnInfo]]
 *          VectorizeColumnInfo contains attributes as follows:
 *          - srcColumnName: comma-separated column names to vectorize (required)
 *                           if set '*', will vectorize all columns
 *          - destColumnName: output column name (required)
 *          - keepSrcColumns: whether keeps source columns; default is true (optional)
 *
 * ==VectorizeColumnInfo==
 * {{{
 * message VectorizeColumnInfo {
 *   required string srcColumnNames = 1 [default = "*"];
 *   required string destColumnName = 2;
 *   optional bool keepSrcColumns = 3 [default = true];
 * }
 * }}}
 */
class VectorizeColumnOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getVectorizeColumn == null) {
      throw new IllegalArgumentException("VectorizeColumnInfo is not set.")
    } else {
      o.getVectorizeColumn
    }

  /**
   * Operate vectorizing multiple columns to one column.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has a vectorized column
   */
  override def operate(inputs: DataFrame): DataFrame = {
    import inputs.sparkSession.implicits._

    Try {
      val srcColumnNames =
        info.getSrcColumnNames match {
          case null | "" =>
            throw new IllegalArgumentException("srcColumnNames is not set.")
          case "*" =>
            inputs.schema.fieldNames
          case _ =>
            info.getSrcColumnNames.split(",").map(_.trim())
        }

      val destColumnName =
        info.getDestColumnName match {
          case null | "" =>
            throw new IllegalArgumentException("destColumnName is not set.")
          case _ =>
            info.getDestColumnName
        }

      val assembler = new VectorAssembler()
        .setInputCols(srcColumnNames)
        .setOutputCol(destColumnName)
      val vectorized = assembler.transform(inputs)

      info.getKeepSrcColumns match {
        case false => vectorized.drop(srcColumnNames: _*)
        case _ => vectorized
      }
    } match {
      case Success(r) =>
        logger.info("vectorize input DataFrame")
        r
      case Failure(e) =>
        logger.info("can't vectorize input DataFrame", e)
        inputs
    }
  }

  private def srcColumnNames(inputs: DataFrame): Array[String] = {
    info.getSrcColumnNames match {
      case null | "" =>
        throw new IllegalArgumentException("featureColumnNames is not set.")
      case "*" =>
        inputs.schema.fieldNames
      case _ =>
        info.getSrcColumnNames.split(",").map(_.trim())
    }
  }
}
