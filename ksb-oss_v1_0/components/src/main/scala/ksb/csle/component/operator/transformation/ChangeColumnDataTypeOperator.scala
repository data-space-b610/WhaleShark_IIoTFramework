package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that changes column data type.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.?]]
 *          ChangeColumnDataTypeInfo contains attributes as follows:
 *          - columName: column name (required)
 *          - dataType: column data type (required)
 *          - more: additional schemes that change column data type (repeated)
 *
 * ==ChangeColumnDataTypeInfo==
 * {{{
 * message ChangeColumnDataTypeInfo {
 *   required string columName = 1;
 *   required FieldType dataType = 2;
 *   repeated ChangeParam more = 3;
 * }
 *
 * message ChangeParam {
 *   required string columName = 1;
 *   required FieldType dataType = 2;
 * }
 * }}}
 */
class ChangeColumnDataTypeOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getChangeColumnDataType == null) {
      throw new IllegalArgumentException(
          "ChangeColumnDataTypeInfo is not set.")
    } else {
      o.getChangeColumnDataType
    }

  /**
   * Operate changing column data type.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe with renamed column.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    import inputs.sparkSession.implicits._

    Try {
      val exprs = buildCastExprs()
      exprs.foldLeft(inputs) { case (src, (columnName, dataType)) =>
        src.withColumn(columnName, inputs(columnName).cast(dataType))
      }
    } match {
      case Success(r) =>
        logger.info("rename input DataFrame")
        r
      case Failure(e) =>
        logger.info("can't rename input DataFrame", e)
        inputs
    }
  }

  private def buildCastExprs(): Seq[(String, types.DataType)] = {
    val buf = ListBuffer[(String, types.DataType)]()

    buf.append((info.getColumName, toSparkDataType(info.getDataType)))

    if (info.getMoreCount > 0) {
      info.getMoreList.foreach { param =>
        buf.append((param.getColumName, toSparkDataType(param.getDataType)))
      }
    }

    buf.toSeq
  }

  private def toSparkDataType(dataType: FieldType): types.DataType = {
    dataType match {
      case FieldType.STRING =>
        types.StringType
      case FieldType.INTEGER =>
        types.IntegerType
      case FieldType.DOUBLE =>
        types.DoubleType
      case FieldType.BOOLEAN =>
        types.BooleanType
      case FieldType.BYTE =>
        types.ByteType
      case FieldType.FLOAT =>
        types.FloatType
      case FieldType.LONG =>
        types.LongType
      case FieldType.TIMESTAMP =>
        types.TimestampType
      case _ =>
        throw new IllegalArgumentException("dataType is not valid.")
    }
  }
}
