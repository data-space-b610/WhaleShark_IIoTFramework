package ksb.csle.component.runner

import org.apache.spark.sql.SparkSession

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._segmentStringToPathMatcher
import akka.http.scaladsl.server.Directives.as
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Directives.entity
import akka.http.scaladsl.server.Directives.extractRequest
import akka.http.scaladsl.server.Directives.path
import akka.http.scaladsl.server.Directives.post
import akka.stream.ActorMaterializer

import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.RunnerProto.OnDemandRunnerInfo

case class KBERequest(queryType: String, query: String)
case class KBEResponse(statuscode: Int, body: String)

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operates as a runner of http server type and delivers a query request to the
 * knowledge base engine via http post request and provides a response from
 * the knowledge base engine.
 *
 * @param o Object that contains message
 *         [[ksb.csle.common.proto.RunnerProto.KBEServingRunnerInfo]]
 *         KBEServingRunnerInfo contains attributes as follows:
 *         - host: host name of runner (required)
 *         - port: port of runner (required)
 *         - uri: url path of runner (optional)
 *         - ssl: whether runner supports ssl (optional)
 *
 * {{{
 *  message KBEServingRunnerInfo {
 *   required string host = 1;
 *   required int32 port = 2;
 *   optional string uri = 3;
 *   optional bool ssl = 4 [default = false];
 * }
 * }}}
 *
 *
 */
class KBEServingRunner(baseInfo: OnDemandRunnerInfo)
    extends BaseRunner[SparkSession, OnDemandRunnerInfo, Unit](baseInfo) {

  private[this] val info = baseInfo.getKbeServingRunner
  private[this] val restServer = new RestServer(info.getPort, predict)
  private[this] var predictPipe: Option[String] => Option[String] = null
  private[this] val query = if (info.getUri.isEmpty()) "query" else info.getUri

  /**
   * Prepare to receive http request toward knowledgebase engine
   *
   * @param  predictPipeline: query request
   */
  override def init(predictPipeline: Any): Unit = {

    if (predictPipeline == null) {
      throw new RuntimeException("Prediction Flow is not set.")
    } else {
      try {
        predictPipe = predictPipeline.asInstanceOf[Option[String] => Option[String]]
      } catch {
        case e: ClassCastException =>
          throw new RuntimeException("Prediction Flow is invalid.")
      }
    }
  }

  private def predict(json: String): String = {
    predictPipe(Some(json)) match {
      case Some(response) =>
        response
      case None =>
        "no response"
    }
  }

  override def run(a: Any): Unit = ()

  override def stop = {
    logger.info("stop rest server")
    restServer.shutdown()
  }

  private[this] var bTerminated: Boolean = false

  def isTerminated() = bTerminated

  private class RestServer(
      port: Int,
      predict: String => String) {

    private[this] implicit val system = ActorSystem("KBEServing")
    private[this] implicit val materializer = ActorMaterializer()
    private[this] implicit val executionContext = system.dispatcher

    // FIXME:: multiple routes

    private[this] val route =
      path(query) {
        post {
          extractRequest { request =>
            request.entity.contentType match {
              case ContentTypes.`application/json` =>
                entity(as[String]) { json =>
                  complete(HttpEntity(ContentTypes.`application/json`, predict(json)))
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      }

    private[this] def errorJson(message: String): String = {
      s"""{\"error\": \"$message\"}"""
    }

    private[this] val bindingFuture = {
      Http().bindAndHandle(route, "0.0.0.0", port.toInt)
    }

    def shutdown() {
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate())
    }
  }

  def getSession: SparkSession = {
    ???
  }
}
