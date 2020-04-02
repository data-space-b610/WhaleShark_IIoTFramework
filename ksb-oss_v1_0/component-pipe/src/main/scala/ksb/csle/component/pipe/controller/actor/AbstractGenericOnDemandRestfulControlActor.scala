package ksb.csle.component.pipe.controller.actor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
import scala.util.Random

import java.util.concurrent.atomic.AtomicReference
import java.util.{Date, Properties}

import org.apache.logging.log4j.scala.Logging
import org.joda.time.DateTime
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import akka.actor._
import akka.util.Timeout
import akka.actor.ActorSelection.toScala

import spray.json._
import spray.http._
import spray.routing.Directives._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing._
import spray.routing.HttpService.runRoute
import play.api.libs.json.Json

import ksb.csle.common.proto.WorkflowProto.SimpleOnDemandControllerInfo
import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.common.base.{Doer, StaticDoer}
import ksb.csle.component.pipe.query.operator._
import ksb.csle.component.pipe.controller.vo.MyJsonProtocol._
import ksb.csle.component.pipe.controller.vo.CumulativeContext

import ksb.csle.component.pipe.runner._

// TODO: Support Key Authentication.
abstract class AbstractGenericOnDemandRestfulControlActor[T](
    p: SimpleOnDemandControllerInfo,
    pipes: List[scala.Function1[BaseRouteContext[T], BaseRouteContext[T]]]
    ) extends Actor with Logging {

  private val defaultUri =
    if (p.hasUri) p.getUri
    else "postQuery"

  def actorRefFactory: ActorContext = context
  implicit val timeout: akka.util.Timeout = Timeout(5.seconds)

  private val serviceStartTime: org.joda.time.DateTime = DateTime.now
  protected val cachedQueryCtx = new AtomicReference[String]()

  protected def applyPipes(ctx: BaseRouteContext[T]): BaseRouteContext[T] =
    pipeAppliers(pipes, ctx)

  @annotation.tailrec
  private[this] def pipeAppliers(
    items: List[scala.Function1[BaseRouteContext[T], BaseRouteContext[T]]], df: BaseRouteContext[T]): BaseRouteContext[T] = {
    items match {
      case Nil => df.asInstanceOf[BaseRouteContext[T]]
      case item :: Nil => pipeAppliers(Nil, item.apply(df))
      case item :: tail => pipeAppliers(tail, item.apply(df))
    }
  }

//  def doQuery(ctxStr: String): BaseRouteContext[T]

  def doQuery(ctxStr: String): String

  override def receive: Actor.Receive = {
    logger.info("Bind route to OnDemandRestfulControlActor.")
    runRoute(route)
  }

  // TODO: Adds authentication.
  val route: spray.routing.Route =
    path("") {
      get { // TODO: Adds description of APIs.
        complete(HttpEntity(
            ContentTypes.`text/plain(UTF-8)`,
            "<h1>KSB FrameWork Serving has been started !!!</h1>"))
      }
    } ~
    path(defaultUri) {
      parameters(
        'userId.as[String],
        'password.as[String],
        'key.as[String]) { (userId, password, key) =>
          post {
              entity(as[String]) { ctxStr =>
                complete(HttpEntity(
                    ContentTypes.`application/json`,
                    doQuery(ctxStr)))
//                    doQuery(ctxStr).toJsonString))
              }
          }
      }
    } ~
    path("reload") {
      post {
        complete {
          context.actorSelection("/user/master") ! ReloadServing()
          "Reloading..."
        }
      }
    } ~
    path("stop") {
      post {
        complete {
          context.system.scheduler.scheduleOnce(1.seconds) {
            context.actorSelection("/user/master") ! StopServing()
          }
          "Shutting down..."
        }
      }
    }

  override def preStart: Unit = {
    logger.info(s"RestfulReaderActor is started.")
  }

  override def postStop: Unit = {
    logger.info("RestfulReaderActor has been stopped")
  }
}
