package ksb.csle.component.runner.actor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
import java.util.concurrent.atomic.AtomicReference

import org.apache.logging.log4j.scala.Logging
import org.joda.time.DateTime

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

import ksb.csle.component.runner._

// TODO: Support Key Authentication.
class ServingActor(readers: Iterable[Long => Option[String]]) extends Actor with Logging {

  val serviceStartTime: org.joda.time.DateTime = DateTime.now

  def actorRefFactory: ActorContext = context
  implicit val timeout: akka.util.Timeout = Timeout(5.seconds)

  private val cachedControlCtx = new AtomicReference[ControlContext](
      ControlContext(DateTime.now, Map.empty[String, String]))

  private def toJson(ctx: ControlContext): String = {
    import ControlContextProtocol._
    ctx.toJson.prettyPrint
  }

  override def receive: Actor.Receive = {
    logger.info("Bind route to serviceActor.")
    runRoute(route)
  }

  // TODO: Adds authentication.
  val route: spray.routing.Route =
    path("") {
      get {
        complete(HttpEntity(
            ContentTypes.`text/plain(UTF-8)`,
            "<h1>Welcome to CSLE Serving</h1>"))
      }
    } ~
    path("query") {
      get {
        def doQuery(): String = {
          Try {
            // FIXME: Handle readers for multiple reading of prediction result.
            Some(ControlContext(DateTime.now,
                Map("predictResult" -> readers.map(_.apply(300)).mkString(", "))))
          } match {
            case Success(result) =>
              result match {
                case Some(ctx) =>
                  logger.debug(s"PredictedResult: " + ctx.params.get("predictResult"))
                  if (ctx.params.get("predictResult").get != "None")
                    cachedControlCtx.set(ctx)
                case _ =>
              }
            case Failure(e) =>
              this.logger.error("doQuery error", e)
          }
          toJson(cachedControlCtx.get)
        }
        complete(HttpEntity(ContentTypes.`application/json`, doQuery))
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
    logger.info(s"ServingActor is started.")
  }

  override def postStop: Unit = {
    logger.info("ServingActor has been stopped")
  }
}
