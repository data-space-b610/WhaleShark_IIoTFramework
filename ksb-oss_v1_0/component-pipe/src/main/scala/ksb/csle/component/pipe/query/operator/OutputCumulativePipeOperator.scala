package ksb.csle.component.pipe.query.operator

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import spray.json._

import ksb.csle.common.base.param.BaseContext
import ksb.csle.common.base.param.BaseCumulativeContext
import ksb.csle.common.base.param.BaseRouteContext

import ksb.csle.common.proto.OndemandControlProto.OnDemandPipeOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.RestfulQueryPipeOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.RestfulQueryPipeOperatorInfo.Method._
import ksb.csle.component.pipe.controller.vo._
import ksb.csle.component.pipe.controller.vo.CumulativeContext

/**
 * Operator that queries to rest server when it is requested
 */
class OutputCumulativePipeOperator[T](
    override val p: OnDemandPipeOperatorInfo,
    override implicit val system: ActorSystem
    ) extends GenericRestfulContextQueryPipeOperator[T](p, system) {

  import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

  override val info: RestfulQueryPipeOperatorInfo = p.getOutputCumulativeOperator

  private def query(
      ctx: BaseCumulativeContext[T, String, Double]): BaseCumulativeContext[T, String, Double] = {
    logger.debug(s"CumulativeOperator called !!")
    val newCtx = ctx.copy
    ctx.getRoute() match {
      case Some(url) =>
        ctx.appendOutput(super.queryUrl(url, newCtx).getOutput())
      case _ =>
        ctx.appendOutput(super.query(newCtx).getOutput())
    }
    ctx
  }

  override def query(ctx: BaseRouteContext[T]) =
    query(ctx.asInstanceOf[BaseCumulativeContext[T, String, Double]])
}
