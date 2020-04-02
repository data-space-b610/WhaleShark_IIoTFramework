package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.apache.spark.ml.feature.StringIndexer

import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.operator.BaseDataOperator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that maps string column to index column.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.StringIndexInfo]]
 *          StringIndexInfo contains attributes as follows:
 *          - srcColumnName: string column (required)
 *          - destColumnName: indexed column (required)
 *          - more: additional indexing schemes (repeated)
 *
 * ==StringIndexInfo==
 * {{{
 * message StringIndexInfo {
 *   required string srcColumnName = 1;
 *   required string destColumnName = 2;
 *   repeated StringIndexParam more = 3;
 * }
 *
 * message StringIndexParam {
 *   required string srcColumnName = 1;
 *   required string destColumnName = 2;
 * }
 * }}}
 */
class StringIndexOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getStringIndex == null) {
      throw new IllegalArgumentException("StringIndexInfo is not set.")
    } else {
      o.getStringIndex
    }

  /**
   * Operate string column indexing.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has indexed columns.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    Try {
      val indexers = buildIndexers()
      indexers.foldLeft(inputs) { case (src, indexer) =>
        indexer.fit(src).transform(src)
      }
    } match {
      case Success(r) =>
        logger.info("index input DataFrame")
        r
      case Failure(e) =>
        logger.info("can't index input DataFrame", e)
        inputs
    }
  }

  private def buildIndexers(): Seq[StringIndexer] = {
    val buf = ListBuffer[StringIndexer]()

    buf.append(
        createIndexer(info.getSrcColumnName, info.getDestColumnName))

    if (info.getMoreCount > 0) {
      info.getMoreList.foreach { param =>
        buf.append(
            createIndexer(param.getSrcColumnName, param.getDestColumnName))
      }
    }

    buf.toSeq
  }

  private def createIndexer(src: String, dest: String): StringIndexer = {
    new StringIndexer()
      .setInputCol(src)
      .setOutputCol(dest)
      .setHandleInvalid("keep")
  }
}
