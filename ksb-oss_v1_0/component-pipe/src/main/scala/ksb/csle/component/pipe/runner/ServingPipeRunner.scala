package ksb.csle.component.pipe.runner

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaj.http.HttpOptions
import org.apache.logging.log4j.scala.Logging

import spray.can.Http
import spray.can.server.ServerSettings
import akka.actor.{Props, Actor, PoisonPill, ActorSystem, Terminated}
import akka.actor.Kill

import akka.stream.ActorMaterializer
import akka.util.Timeout
import akka.io.IO
import akka.actor.ActorRef
import akka.pattern.ask

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto.{OnDemandRunnerInfo, WebserviceRunnerInfo}

import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.utils.WorkflowUtils
import ksb.csle.common.utils.SparkUtils

/**
 * :: Experimental ::
 *
 * Runner that serves pipelined query when client requests through restful interface
 */
final class ServingPipeRunner(
    o: OnDemandRunnerInfo) extends BaseRunner[ActorSystem, OnDemandRunnerInfo, BaseResult](o) {

  private val runnerInfo: WebserviceRunnerInfo = o.getOneofRunnersCase match {
    case OnDemandRunnerInfo.OneofRunnersCase.WEBRUNNER => o.getWebRunner
    case OnDemandRunnerInfo.OneofRunnersCase.WEBPIPERUNNER => o.getWebPipeRunner
    case _  =>
      throw new IllegalArgumentException("not supported argument type.")
  }
  import ServingPipeRunner._
  override def getSession: ActorSystem  = actorSystem
  var servingMaster: ActorRef = null

  override def run(actor: Any): BaseResult = {
    implicit val timeout = Timeout(5.seconds)
    servingMaster ? StartServing()
    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  override def init(actor: Any): BaseResult = {
    servingMaster = actorSystem.actorOf(
        Props(classOf[ServingMaster], runnerInfo, actor.asInstanceOf[ActorRef]),
        "master")
    DefaultResult("s","p","o").asInstanceOf[BaseResult]
  }

  override def stop: Unit = (actorSystem.shutdown)
}

object ServingPipeRunner extends Logging {
  def apply(
      serviceInfo: OnDemandRunnerInfo): BaseRunner[ActorSystem, OnDemandRunnerInfo, BaseResult] = new ServingPipeRunner(serviceInfo)

  private[pipe] implicit val actorSystem: ActorSystem = ActorSystem("ksb-pipe-serving")
  private[pipe] implicit val materializer = ActorMaterializer()
  private[pipe] implicit val executionContext = actorSystem.dispatcher
}

sealed case class StartServing()
sealed case class StopServing()
sealed case class BindServing()
sealed case class ReloadServing()

/**
 * Master Actor for Serving
 */
class ServingMaster(
    runnerInfo: WebserviceRunnerInfo, actor: ActorRef) extends Actor with Logging {

  implicit val timeout = Timeout(5.seconds)
  var sprayHttpListener: Option[ActorRef] = None
  var currentServingActor: Option[ActorRef] = None
  var retry = 3

  val (ip, port, ssl) = (runnerInfo.getHost, runnerInfo.getPort, runnerInfo.getSsl)
  val protocol = if (ssl) "https://" else "http://"
  val serverUrl = s"${protocol}${ip}:${port}"

  private def undeploy(ip: String, port: Int): Unit = {
    logger.info( s"Undeploying any existing engine instance at $serverUrl")
    try {
      val code = scalaj.http.Http(s"$serverUrl/stop")
        .option(HttpOptions.allowUnsafeSSL)
        .method("POST").asString.code
      code match {
        case 200 => ()
        case 404 => logger.error(
          s"Another process is using ${serverUrl}. Unable to undeploy.")
        case _ => logger.error(
          s"Another process is using ${serverUrl}, or an existing " +
          s"engine server is not responding properly (HTTP $code). " +
          "Unable to undeploy.")
      }
    } catch {
      case e: java.net.ConnectException =>
        logger.warn(s"Nothing at ${serverUrl}")
      case _: Throwable =>
        logger.error("Another process might be occupying " +
          s"${ip}:${port}. Unable to undeploy.")
    }
  }

  override def receive: Actor.Receive = {
    case x: StartServing =>
      currentServingActor = Some(actor)
      undeploy(runnerInfo.getHost, runnerInfo.getPort)
      self ! BindServing()

    case x: BindServing =>
      import ServingPipeRunner._
      currentServingActor map { actor =>
        IO(Http) ! Http.Bind(
            actor,
            interface = ip,
            port = port)
        logger.info(s"ServingMaster binds ServingAgent to ${serverUrl}.")
      } getOrElse {
        logger.error(s"ServingMaster could not bind ServingAgent to ${serverUrl}.")
      }

    case x: StopServing =>
      sprayHttpListener.map { l =>
        logger.info("Server is shutting down.")
        l ! Http.Unbind(5.seconds)
        context.system.shutdown()
        logger.info("ServingAgent has been stopped")
      } getOrElse {
        logger.warn("No ServingAgent is running.")
      }

    case x: ReloadServing =>  // FIXME: Fix binding error.
      logger.info("Reload serving command received.")
      val latestServingInstance = Some(WebserviceRunnerInfo.getDefaultInstance)
      latestServingInstance map { latest =>
        sprayHttpListener.map { l =>
          l ! Http.Unbind(5.seconds)
          implicit val system = context.system
          val settings = ServerSettings(system)
          IO(Http) ! Http.Bind(
            actor,
            interface = ip,
            port = port,
            settings = Some(settings.copy(sslEncryption = ssl)))
          currentServingActor.get ! Kill
          currentServingActor = Some(actor)
        } getOrElse {
          logger.warn("No active server is running. Abort reloading.")
        }
      } getOrElse {
        logger.warn(
          s"No latest completed engine instance. Abort reloading.")
      }

    case x: Http.Bound =>
      logger.info(s"ServingActor is up and running at ${serverUrl}.")
      sprayHttpListener = Some(sender)

    case x: Http.CommandFailed =>
      if (retry > 0) {
        retry -= 1
        logger.error(s"Bind failed. Retrying... ($retry more trial(s))")
        context.system.scheduler.scheduleOnce(1.seconds) {
          self ! BindServing()
        }
      } else {
        logger.error("Bind failed. Shutting down.")
        context.system.shutdown()
      }
  }
}
