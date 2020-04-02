package ksb.csle.component.controller

import scala.collection.JavaConversions._
import org.apache.logging.log4j.scala.Logging

import akka.actor.ActorRef
import akka.actor._

import ksb.csle.common.proto.WorkflowProto.OnDemandControllerInfo
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.controller.BaseOnDemandStreamServingController
import ksb.csle.common.base.operator.BaseGenericMutantOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.utils.response.KbeResponse

import ksb.csle.component.runner.actor.ServingActor
import ksb.csle.component.runner.actor.MlKbServingActor

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that pipelines data from reader to operators when a query arrives.
 * And responses pipelined result to the query submitter.
 *
 * @param runner Runner object that is executed when engine starts.
 *               Restful server usually work with [[ServingWithKbController]].
 * @param i      A List of reader objects that carries input file path.
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining.
 * @param ops    a list of [[BaseGenericMutantOperator]] objects that passes string object.
 */
class ServingWithKbController[T, R, SI, SO](
    override val runner: BaseRunner[_,_,_],
    override val i: List[BaseReader[T,_,SI]],
    override val p: OnDemandControllerInfo,
    override val ops : List[BaseGenericMutantOperator[_, Option[String], _]]
    ) extends BaseOnDemandStreamServingController[T, OnDemandControllerInfo, R, SI, SO](runner, i, p, ops) {

  private[this] val info = p.getServingWithKbController

  private def actor: ActorRef = {
    val readers: Iterable[T] = i map {_.read(300l.asInstanceOf[SI])}
    val operators: List[Option[String] => Option[KbeResponse]]
      = (readers, ops).zipped.toList.map { rOps =>
        rOps._2.operate(Some("")).asInstanceOf[Option[String]=>Option[KbeResponse]]
        }.toList
    val rOps = (readers, operators).zipped.toList
    if (rOps.size > 0)
      ServingWithKbControllerActor.createCompositeActorWithEngine(
          runner.getSession.asInstanceOf[ActorSystem],
          rOps.get(0))
    else
      ServingWithKbControllerActor.createReadingActorWithEngine(
        runner.getSession.asInstanceOf[ActorSystem],
        readers.asInstanceOf[Iterable[Long => Option[String]]])
  }

  override def serve(): BaseResult = {
    // FIXME: Generate actors for each reader and operator pipeline..
    runner.init(actor)
    runner.run()
    DefaultResult("s","p","o").asInstanceOf[BaseResult]
  }
}

object ServingWithKbControllerActor extends Logging {

  private[controller] def createReadingActorWithEngine(
      actorSystem: ActorSystem,
      readers: Iterable[Long => Option[String]]): ActorRef = {
    actorSystem.actorOf(
        Props(classOf[ServingActor], readers))
  }

  private[controller] def createCompositeActorWithEngine[T](
      actorSystem: ActorSystem,
      rOp: (T, Option[String] => Option[KbeResponse])): ActorRef = {
    actorSystem.actorOf(Props(classOf[MlKbServingActor], rOp))
  }
}
