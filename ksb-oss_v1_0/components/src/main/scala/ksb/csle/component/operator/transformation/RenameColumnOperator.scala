package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that renames existing column.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.RenameColumnInfo]]
 *          RenameColumnInfo contains attributes as follows:
 *          - existingName: existing name (required)
 *          - newName: new name (required)
 *          - more: additional renaming schemes (repeated)
 *
 * ==RenameColumnInfo==
 * {{{
 * message RenameColumnInfo {
 *   required string existingName = 1;
 *   required string newName = 2;
 *   repeated RenamingParam more = 3;
 * }
 *
 * message RenamingParam {
 *   required string existingName = 1;
 *   required string newName = 2;
 * }
 * }}}
 */
class RenameColumnOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getRenameColumn == null) {
      throw new IllegalArgumentException("RenameColumnInfo is not set.")
    } else {
      o.getRenameColumn
    }

  /**
   * Operate renaming one or more column.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has prediction output columns.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    import inputs.sparkSession.implicits._

    Try {
      val exprs = buildRenameExprs()
      exprs.foldLeft(inputs) { case (src, (existingName, newName)) =>
        src.withColumnRenamed(existingName, newName)
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

  private def buildRenameExprs(): Seq[(String, String)] = {
    val buf = ListBuffer[(String, String)]()

    buf.append((info.getExistingName, info.getNewName))

    if (info.getMoreCount > 0) {
      info.getMoreList.foreach { param =>
        buf.append((param.getExistingName, param.getNewName))
      }
    }

    buf.toSeq
  }
}
