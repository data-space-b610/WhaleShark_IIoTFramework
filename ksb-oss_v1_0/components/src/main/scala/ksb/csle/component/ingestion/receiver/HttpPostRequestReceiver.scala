package ksb.csle.component.ingestion.receiver

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Success, Failure}

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

import ksb.csle.component.ingestion.common._

/**
 * HTTP Streaming data receiver based on Spark Streaming.
 *
 * @param name      Receiver name
 * @param interface IP address of HTTP server
 * @param port      Port number of HTTP server
 * @param handler   User defined function that handles HTTP POST request
 */
final class HttpPostRequestReceiver(
  name: String,
  interface: String,
  port: Int,
  handler: HttpIngestionRequest => HttpIngestionResponse =
    defaultHttpIngestionHandler) extends Receiver[String](
        StorageLevel.MEMORY_AND_DISK_2) with Logging {

  import akka.actor.ActorSystem
  import akka.stream.ActorMaterializer
  import akka.http.scaladsl.Http
  import akka.http.scaladsl.model._
  import akka.http.scaladsl.model.HttpMethods._
  import akka.http.scaladsl.server.Directives._

  case class ServerStopArgs(
    bindingFuture: Future[Http.ServerBinding],
    actorSystem: ActorSystem)

  var serverStopArgs: ServerStopArgs = null

  /**
   * This method is called when the Spark Streaming is started.
   */
  override def onStart() {
    serverStopArgs = startHttpServer()
  }

  /**
   * This method is called when the Spark Streaming is stopped.
   */
  override def onStop() {
    if (serverStopArgs != null) {
      stopHttpServer(serverStopArgs)
    }
  }

  /**
   * specify a preferred location (hostname).
   */
  override def preferredLocation: Option[String] = {
    if (interface == null) None
    else Some(interface)
  }

  private def startHttpServer(): ServerStopArgs = {
    implicit val system = ActorSystem(name)
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val route = withoutSizeLimit {
      path("") {
        post {
          extractRequest { request =>
            entity(as[String]) { body =>
              val mediaTypeValue = request.entity.contentType.mediaType.value
              val ireq = HttpIngestionRequest(body, Some(mediaTypeValue),
                  Some(request.headers.map(h => h.name -> h.value).toMap))

              val irsp = handler(ireq)
              if (irsp.statusCode == StatusCodes.OK) {
                store(irsp.data)

                if (logger.delegate.isDebugEnabled()) {
                  val data =
                    if (irsp.data.length() > 128) {
                      irsp.data.substring(0, 128)
                    } else {
                      irsp.data
                    }
                  val tid = Thread.currentThread().getId()
                  logger.debug(s"[$tid] store: $mediaTypeValue, $data}")
                }
              }

              complete(irsp.statusCode, irsp.message)
            }
          }
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", port)
    bindingFuture.onComplete {
      case Success(binding) =>
        logger.info(s"'${name}' bind to ${binding.toString()}")
      case Failure(e) =>
        val message = s"'${name}' bind error: ${e.getMessage}"
        logger.error(message, e)
        reportError(message, e)
    }

    ServerStopArgs(bindingFuture, system)
  }

  private def stopHttpServer(args: ServerStopArgs) {
    implicit val system = args.actorSystem
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    args.bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }
}
