package ksb.csle.component.pipe.query.operator

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import spray.json._

import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.common.proto.OndemandControlProto.OnDemandPipeOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.RestfulQueryPipeOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.RestfulQueryPipeOperatorInfo.Method._
import ksb.csle.component.pipe.controller.vo._

/**
 * Operator that queries to rest server when it is requested
 */
class OutputRestfulContextQueryPipeOperator[T](
    override val p: OnDemandPipeOperatorInfo,
    override implicit val system: ActorSystem
    ) extends GenericRestfulContextQueryPipeOperator[T](p, system) {

  import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._

  override def query(ctx: BaseRouteContext[T]): BaseRouteContext[T] = {
    ctx.getRoute() match {
      case Some(url) =>
        super.queryUrl(url, ctx)
      case _ =>
        super.query(ctx)
    }
  }
}
