package ksb.csle.component.pipe.controller

import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import akka.actor.{ Actor, ActorLogging, ActorSystem }
import akka.actor.ActorRef
import akka.actor._

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.WorkflowProto.OnDemandControllerInfo
import ksb.csle.common.proto.RunnerProto.OnDemandRunnerInfo
import ksb.csle.common.proto.OndemandControlProto.OnDemandOperatorInfo

import ksb.csle.common.base.param.DeviceCtxCtlCase
import ksb.csle.common.base.pipe.reader.BasePipeReader
import ksb.csle.common.base.pipe.writer.BasePipeWriter
import ksb.csle.common.base.pipe.operator.BaseGenericPipeOperator
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.pipe.controller.BaseQueryPipeController
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.result.DefaultResult

import ksb.csle.component.pipe.controller.actor.OnDemandGenericControlActor
import ksb.csle.component.pipe.controller.vo._

/**
 * :: Experimental ::
 *
 * Controller that manipulate reader, operator, and writer handles
 * when it is requested.
 */
class OnDemandGenericController(
  override val runner: BaseRunner[ActorSystem, OnDemandRunnerInfo, Any],
  override val ins: Seq[BasePipeReader[DeviceCtxCtlCase, OnDemandReaderInfo, ActorSystem]],
  override val p: OnDemandControllerInfo,
  override val ops: Seq[BaseGenericPipeOperator[_, _, DeviceCtxCtlCase, DeviceCtxCtlCase, ActorSystem]]
  ) extends BaseQueryPipeController[DeviceCtxCtlCase, DeviceCtxCtlCase, DeviceCtxCtlCase, OnDemandControllerInfo, ActorSystem](
      runner, ins, p, ops) {

  private def controlActor: ActorRef =
    runner.getSession.actorOf(Props(classOf[OnDemandGenericControlActor], applyPipes))

  override def init = {
    super.init()
  }

  override def progress: BaseResult = {
    super.progress()
    runner.init(controlActor)
    runner.run()
    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  override def stop: Any = runner.getSession.terminate()
}
