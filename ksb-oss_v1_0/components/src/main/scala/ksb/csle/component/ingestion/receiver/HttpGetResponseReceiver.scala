package ksb.csle.component.ingestion.receiver

import java.io.Serializable

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.StreamingContext

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import akka.stream.ActorMaterializer
import play.api.libs.ws.{ WSRequest, WSResponse }
import play.api.libs.ws.ahc.AhcWSClient

/**
 * :: ApplicationDeveloperApi ::
 *
 * After sending a get request to the http server,
 * it stores the received response in spark memory.
 *
 *  @param name name of http post receiver actor
 *  @param url target http server url includes hostname, port, and path
 *  @param params http params
 *  @param headers http headers
 */
// TODO take Some(params), Some(headers)
final class HttpGetResponseReceiver(val name: String,
                                    val url: String,
                                    val params: Map[String, String],
                                    val headers: Map[String, String])
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  case class StopArgs(system: ActorSystem, wsClient: AhcWSClient)

  var stopArgs: StopArgs = null

  val REQUEST_TIMEOUT: FiniteDuration = 30 seconds

  /**
   * Start to receive response from http server.
   */
  override def onStart(): Unit = {
    stopArgs = receive()
  }

  /**
   * Stop to receive response from http server.
   */
  override def onStop(): Unit = {
    if (stopArgs != null) {
      stop(stopArgs)
    }
  }

  private def receive(): StopArgs = {

    implicit val system = ActorSystem(name)
    implicit val materializer = ActorMaterializer()
    val wsClient = AhcWSClient()

    val ps = params.toList
    val hs = headers.toList

    val request: WSRequest = wsClient.url(url)
      .addQueryStringParameters(ps: _*)
      .addHttpHeaders(hs: _*)

    val result = getOperation(request)
    logger.info("result: " + result.body)

    val status = result.status
    if (status == 200 | status == 201) {
      logger.info("Store: success!!")
      store(result.body)
    } else {
      logger.info("Store: failed, " + result.body)
    }
    StopArgs(system, wsClient)
  }

  private def getOperation(wsRequest: WSRequest): WSResponse = {
    val futureResult: Future[WSResponse] = wsRequest.get.map {
      response =>
        response
    }
    Await.result(futureResult, REQUEST_TIMEOUT)
  }

  private def stop(stopArgs: StopArgs) = {
    stopArgs.wsClient.close()
    stopArgs.system.terminate()
  }

}
