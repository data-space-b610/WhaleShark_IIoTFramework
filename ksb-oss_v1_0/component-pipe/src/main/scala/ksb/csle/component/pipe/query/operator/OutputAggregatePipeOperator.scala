package ksb.csle.component.pipe.query.operator

import scala.collection.immutable.Seq
import scala.collection.JavaConversions._

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import spray.json._

import ksb.csle.common.base.param.BaseCumulativeContext
import ksb.csle.common.proto.OndemandControlProto.OnDemandPipeOperatorInfo
import ksb.csle.common.base.pipe.query.operator.BaseContextQueryPipeOperator

import ksb.csle.component.pipe.controller.vo._

/**
 * Operator that queries to rest server when it is requested
 */
class OutputAggregatePipeOperator[T](
    override val p: OnDemandPipeOperatorInfo,
    override implicit val system: ActorSystem
    ) extends BaseContextQueryPipeOperator[BaseCumulativeContext[T, String, Double], ActorSystem](p, system) {

  import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

  private val info = p.getOutputAggregateOperator
  private val method = info.getMethod
  private val separator: String =
    if (info.hasSeparator()) info.getSeparator else " "
  private val arrayDepth: Int =
    if (info.hasArrayDepth()) info.getArrayDepth else 1
  private val regex = "[\\[|\\]]".r

  private def parse(item: String): String => Array[Double] = item =>
    regex.replaceAllIn(item, "")
    .split(separator)
    .map(_.replace("\"","").toDouble)

  override def query(ctx: BaseCumulativeContext[T, String, Double]): BaseCumulativeContext[T, String, Double] = {
    logger.debug(s"Query opId #${p.getId} called !!")
    logger.debug(s"Data for query opId #${p.getId} : ${ctx.toJsonString}")
    ctx.aggregateOutput(method, parse(""))
  }
}
