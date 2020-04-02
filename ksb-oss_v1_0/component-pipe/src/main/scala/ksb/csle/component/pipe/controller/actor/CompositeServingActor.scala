package ksb.csle.component.pipe.controller.actor

import spray.json._

import scala.util.{Try, Success, Failure}

import ksb.csle.common.proto.WorkflowProto.SimpleOnDemandControllerInfo
import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.component.pipe.controller.vo._

class CompositeServingActor[T](
    p: SimpleOnDemandControllerInfo,
    pipes: List[scala.Function1[BaseRouteContext[T], BaseRouteContext[T]]]
    ) extends AbstractGenericOnDemandRestfulControlActor[T](p, pipes) {

  import ksb.csle.component.pipe.controller.vo.MyRouteJsonProtocol._

  override def doQuery(ctxStr: String): String = {
    logger.debug(s"CompositeServingActor called !!")
    val (inCtx, isCase) = Try {
      new RouteContext(ctxStr)
    } match {
      case Success(ctx) => (ctx.asInstanceOf[BaseRouteContext[T]], true)
      case Failure(e) =>
        (new RouteContext(RouteCtxCase(None, None, Some(ctxStr), None)), false)
    }
    val outCtx = applyPipes(inCtx.asInstanceOf[BaseRouteContext[T]])
    if (isCase) outCtx.toJsonString
    else outCtx.getOutput.getOrElse("")
  }
}
