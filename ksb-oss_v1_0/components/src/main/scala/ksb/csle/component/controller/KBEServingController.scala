package ksb.csle.component.controller

import java.lang.reflect.ParameterizedType
import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.utils._
import ksb.csle.common.base.{Doer, StaticDoer}
import ksb.csle.common.base.controller.BaseOnDemandServingController
import ksb.csle.common.base.operator.BaseGenericMutantOperator
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.controller.BaseController
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.runner.KBEServingRunner

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that pipelines data from operator to operators.
 * This controll is triggered when query has been arrived
 * and pipelines a series of subsequent operators.
 *
 * @param runner Runner object that is executed when engine starts.
 * @param p      Message object that has on of on-demand style message object.
 * @param ops    a list of [[BaseGenericMutantOperator]] objects.
 */
final class KBEServingController[T, R](
    override val runner: BaseRunner[_, _, _],
    override val p: OnDemandControllerInfo,
    override val ops: List[BaseGenericMutantOperator[_, R, R]]
    ) extends BaseOnDemandServingController[T, OnDemandControllerInfo, R](runner, p, ops) {

  private[this] val predictFlow: Object = p.getOneofControllersCase match {
    case OnDemandControllerInfo.OneofControllersCase.ONDEMANDGENERICCONTROLLER =>
      (inputs: R) => pipeOperators(runner, ops, inputs)
    case OnDemandControllerInfo.OneofControllersCase.TENSORFLOWSERVINGCONTROLLER =>
      (inputs: R) => pipeOperators(runner, ops, inputs)
    case OnDemandControllerInfo.OneofControllersCase.SERVINGWITHKBCONTROLLER =>
      (inputs: R) => pipeOperators(runner, ops, inputs)
    case OnDemandControllerInfo.OneofControllersCase.KBESERVINGCONTROLLER =>
      (inputs: R) => pipeOperators(runner, ops, inputs)
    case OnDemandControllerInfo.OneofControllersCase.ONEOFCONTROLLERS_NOT_SET =>
      throw new RuntimeException("Controller info is not set.")
  }

  override def serve() = {
    runner match {
      case tsruuner: KBEServingRunner =>
        tsruuner.init(predictFlow)
        tsruuner.run()
      case _ =>
        runner.init()
        runner.run()
    }
    DefaultResult("s","p","o").asInstanceOf[BaseResult]
  }

  override def stop: Any = {
    super.stop()
  }
}
