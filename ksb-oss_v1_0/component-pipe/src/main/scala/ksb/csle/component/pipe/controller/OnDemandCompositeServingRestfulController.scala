package ksb.csle.component.pipe.controller

import akka.actor.{ Actor, ActorRef, ActorSystem }
import akka.actor._
import spray.json._

import ksb.csle.common.proto.WorkflowProto.OnDemandControllerInfo
import ksb.csle.common.proto.RunnerProto.OnDemandRunnerInfo
import ksb.csle.common.base.pipe.operator.BaseGenericPipeOperator
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.param.BaseRouteContext

import ksb.csle.component.pipe.controller.actor.CompositeServingActor
import ksb.csle.component.pipe.controller.vo._
import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

/**
 * :: Experimental ::
 *
 * Controller that manipulates restful query operators.
 */
class OnDemandCompositeServingRestfulController[T](
  override val runner: BaseRunner[ActorSystem, OnDemandRunnerInfo, Any],
  override val p: OnDemandControllerInfo,
  override val ops: Seq[BaseGenericPipeOperator[
    BaseRouteContext[T]=>BaseRouteContext[T],
    BaseRouteContext[T], BaseRouteContext[T], _, ActorSystem]]
  ) extends AbstractGenericOnDemandRestfulController[T](runner, p, ops) {

  private val info = p.getOnDemandCompositeServingController
  private val actorClzName =
    if (info.hasRestfulActorName()) info.getRestfulActorName
    else "ksb.csle.component.pipe.controller.actor.CompositeServingActor"

  override def controlActor: ActorRef = {
    runner.getSession.actorOf(
      Props(Class.forName(actorClzName), p.getOnDemandCompositeServingController, pipes))
  }
}
