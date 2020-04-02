package ksb.csle.component.pipe.controller.actor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
import scala.util.Random
import java.util.concurrent.atomic.AtomicReference
import java.util.{Date, Properties}

import org.apache.logging.log4j.scala.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
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
import play.api.libs.json.Json

import ksb.csle.component.pipe.runner._
import ksb.csle.common.base.param.DeviceCtxCtlCase
import ksb.csle.common.base.param.MyJsonProtocol._

final class KafkaSender(topic: String, key: String){
  import play.api.libs.json.Json

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("zookeeper.connect", "localhost:2181")
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  private[pipe] def send(ctxMsg: DeviceCtxCtlCase) = {
    implicit val writes = Json.writes[DeviceCtxCtlCase]
    val data = new ProducerRecord[String, String](key, Json.toJson(ctxMsg).toString)
    producer.send(data)
  }
}

object KafkaSender {
  def apply(topic: String, key: String) = new KafkaSender(topic, key)
}

/**
 * :: Experimental ::
 *
 * Actor that receives query and starts to control when it is requested.
 */
class OnDemandGenericControlActor(
    pipes: scala.Function0[DeviceCtxCtlCase]) extends Actor with Logging {

  def actorRefFactory: ActorContext = context
  implicit val timeout: akka.util.Timeout = Timeout(5.seconds)

  val serviceStartTime: org.joda.time.DateTime = DateTime.now
  private val cachedControlCtx = new AtomicReference[DeviceCtxCtlCase](
      DeviceCtxCtlCase("","","","",""))
  private val messenger = KafkaSender("topicQuery", "myKey")

  private def doQuery(ctx: String) = {
    val deviceCtx = ctx.asJson.convertTo[DeviceCtxCtlCase]
    messenger.send(deviceCtx)
    cachedControlCtx.set(pipes.apply())
    cachedControlCtx.get.toJson.toString()
  }

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
    path("postQuery") {
      parameters(
        'userId.as[String],
        'password.as[String],
        'key.as[String]) { (userId, password, key) =>
          post {
              entity(as[String]) { deviceCtx =>
                complete(HttpEntity(ContentTypes.`application/json`, doQuery(deviceCtx)))
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
