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

import ksb.csle.common.base.pipe.reader.BasePipeReader
import ksb.csle.common.base.pipe.writer.BasePipeWriter
import ksb.csle.common.base.pipe.operator.BaseGenericPipeOperator
import ksb.csle.common.base.pipe.controller.BaseQueryWoReaderPipeController
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.pipe.controller.vo._

abstract class AbstractGenericOnDemandRestfulController[T](
  override val runner: BaseRunner[ActorSystem, OnDemandRunnerInfo, Any],
  override val p: OnDemandControllerInfo,
  override val ops: Seq[BaseGenericPipeOperator[
    BaseRouteContext[T]=>BaseRouteContext[T], BaseRouteContext[T], BaseRouteContext[T], _, ActorSystem]]
  ) extends BaseQueryWoReaderPipeController[
    BaseRouteContext[T]=>BaseRouteContext[T], BaseRouteContext[T], BaseRouteContext[T], OnDemandControllerInfo, ActorSystem](
        runner, p, ops) {

  protected val pipes = getPipes(T=>T)

  protected def controlActor: ActorRef

  override def init = {
    logger.info("Engine is initializing...")
    super.init()
    logger.info("Actor creating...")
    val actor = controlActor
    logger.info("Actor created...")
    runner.init(actor)
  }

  override def progress: BaseResult = {
    logger.info("Engine is running...")
    super.progress()
    runner.run()

    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  override def stop: Any = runner.getSession.terminate()
}
