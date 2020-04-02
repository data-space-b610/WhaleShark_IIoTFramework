package ksb.csle.component.pipe.controller.actor

import scala.util.{Try, Success, Failure}

import spray.json._

import ksb.csle.common.proto.WorkflowProto.SimpleOnDemandControllerInfo
import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.component.pipe.controller.vo._

class EnsembleServingActor[T](
    p: SimpleOnDemandControllerInfo,
    pipes: List[scala.Function1[BaseRouteContext[T], BaseRouteContext[T]]]
    ) extends AbstractGenericOnDemandRestfulControlActor[T](p, pipes) {

  import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

  override def doQuery(ctxStr: String): String = {
    logger.debug(s"EnsembleServingActor called !!")
    val (inCtx, isCase) = Try {
      new CumulativeContext(ctxStr)
    } match {
      case Success(ctx) => (ctx.asInstanceOf[BaseRouteContext[T]], true)
      case Failure(e) =>
        (new CumulativeContext(EnsembleCase(None, None, Some(Array(ctxStr)), None)), false)
    }
    val outCtx = applyPipes(inCtx.asInstanceOf[BaseRouteContext[T]])
    if (isCase) outCtx.toJsonString
    else outCtx.getOutputList.getOrElse("")
  }
}
