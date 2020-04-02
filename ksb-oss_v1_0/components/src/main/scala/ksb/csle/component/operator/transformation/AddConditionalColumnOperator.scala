package ksb.csle.component.operator.transformation

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.parser._

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that adds new column in the given rules.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AddConditionalColumnInfo]]
 *          AddConditionalColumnInfo contains attributes as follows:
 *          - newColumnName: new column name (required)
 *          - rules: rule list to set value to new column (optional)
 *          AddColumnRule contains attributes as follows:
 *          - condition: condition expression in SQL format (required)
 *          - value: value if condition matched (required)
 *
 * ==AddConditionalColumnInfo==
 * {{{
 * message AddConditionalColumnInfo {
 *   required string newColumnName = 1;
 *   repeated AddColumnRule rules = 2;
 * }
 *
 * message AddColumnRule {
 *   required string condition = 1;
 *   required string value = 2;
 * }
 * }}}
 */
class AddConditionalColumnOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  private[this] val info =
    if (o.getAddConditionalColumn == null) {
      throw new IllegalArgumentException("AddConditionalColumnInfo is not set.")
    } else {
      o.getAddConditionalColumn
    }

  private[this] val newColumnName = info.getNewColumnName match {
    case null | "" =>
      throw new IllegalArgumentException("newColumnName is not set.")
    case _ =>
      info.getNewColumnName
  }

  /**
   * Operate adding new column in the given rules.
   *
   * @param  inputs    Input dataframe
   * @return DataFrame Output dataframe has new column.
   */
  override def operate(inputs: DataFrame): DataFrame = {
    Try {
      inputs.withColumn(
          newColumnName,
          buildWhenExpr(inputs.sparkSession.sessionState.sqlParser))
    } match {
      case Success(r) =>
        logger.info(s"add conditional column '$newColumnName'")
        r
      case Failure(e) =>
        logger.info("can't add conditional column", e)
        inputs
    }
  }

  private def buildWhenExpr(sqlParser: ParserInterface): Column = {
    val exprs = info.getRulesList.map(toWhenExpr(_, sqlParser))
    if (exprs.isEmpty) {
      throw new IllegalArgumentException("rules are not set.")
    }

    val last = exprs.last
    val heads = exprs.take(exprs.length - 1)
    heads.foldRight(last) { case (ifPart, thenPart) =>
      ifPart.otherwise(thenPart)
    }
  }

  private def toWhenExpr(rule: AddColumnRule,
      sqlParser: ParserInterface): Column = {
    (rule.getCondition.isEmpty(), rule.getValue.isEmpty()) match {
      case (false, false) =>
        when(new Column(sqlParser.parseExpression(rule.getCondition)),
            rule.getValue)
      case _ =>
        throw new IllegalArgumentException("Rule is not valid.")
    }
  }
}
