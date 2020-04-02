package ksb.csle.component.pipe.query.operator

import scala.collection.JavaConversions._

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import spray.json._

import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.common.proto.OndemandControlProto.OnDemandPipeOperatorInfo
import ksb.csle.common.base.pipe.query.operator.BaseContextQueryPipeOperator
import ksb.csle.component.pipe.controller.vo._

/**
 * Operator that queries to rest server when it is requested
 */
class RouteMappingPipeOperator[T](
    override val p: OnDemandPipeOperatorInfo,
    override implicit val system: ActorSystem
    ) extends BaseContextQueryPipeOperator[BaseRouteContext[T], ActorSystem](p, system) {

  import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

  private val routeMap = p.getRouteMappingOperator.getRouteMapList.map { rMap =>
    rMap.getIdx -> rMap.getRoute
  }.toMap

  override def query(ctx: BaseRouteContext[T]): BaseRouteContext[T] = {
    logger.debug(s"Query opId #${p.getId} called !!")
    logger.debug(s"Data for query opId #${p.getId} : ${ctx.toJsonString}")
    logger.debug(s"Index for Route #${p.getId} : ${ctx.getRoute().get}")
    val route = routeMap.get(ctx.getRoute.get)
    ctx.setRoute(route)
    ctx
  }
}
