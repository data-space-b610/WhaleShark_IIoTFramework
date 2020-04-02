package ksb.csle.examples.device

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Success, Failure }
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.util.ByteString
import spray.json._
import org.apache.logging.log4j.scala.Logging
import akka.http.scaladsl.model.Uri.apply
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ahc.AhcWSClient

object HUEProxySample extends Logging {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  private val wsClient = AhcWSClient()

  def queryParamContext(): String = {
    try {
      this.logger.debug("queryParamContext")

      val responseFuture: Future[HttpResponse] = Http().singleRequest(
        HttpRequest(uri = "http://localhost:18080/query"))
      this.logger.debug("responseFuture = " + responseFuture)

      val temp = Await.ready(responseFuture, 3 seconds)
      this.logger.debug("ready = " + temp)

      val response = Await.result(responseFuture, 3 seconds)
      this.logger.debug(response.status)

      val bsFuture: Future[ByteString] =
        response.entity.toStrict(3 seconds).map(_.data)
      val contentType = response.entity.contentType
      val content = Await.result(bsFuture, 3 seconds).decodeString("UTF-8")
      this.logger.debug(s"got a ParamContexts\n${contentType}\n${content}")
      content
    } catch {
      case e: Exception =>
        this.logger.error("queryParamContext() error", e)
        null
    }
  }

  def setLightState(json: String) {
    try {
      //      val root = json.parseJson.asJsObject
      //      val params = root.getFields("params")(0).asJsObject
      //  
      //      val hue = params.getFields("hue")(0).asInstanceOf[JsString].value
      //      val on = params.getFields("on")(0).asInstanceOf[JsString].value
      //      val sat = params.getFields("sat")(0).asInstanceOf[JsString].value
      //      val bri = params.getFields("bri")(0).asInstanceOf[JsString].value
      //
      //      val root = json.parseJson.asJsObject
      //      val params = root.getFields("controls")(0).asJsObject
      //  
      //      val hue = params.getFields("hue")(0).asInstanceOf[JsString].value
      //      val on = params.getFields("on")(0).asInstanceOf[JsString].value
      //      val sat = params.getFields("sat")(0).asInstanceOf[JsString].value
      //      val bri = params.getFields("bri")(0).asInstanceOf[JsString].value

      logger.info(s"[HUE] Received control message: $json")
      val originalMsg = json.parseJson.asInstanceOf[JsArray]
      val parsedMsg = originalMsg.elements.head.asJsObject
      val params = parsedMsg.getFields("controls")(0).asJsObject
      val on = params.getFields("on")(0).asInstanceOf[JsString].value.toBoolean
      val bri = params.getFields("bri")(0).asInstanceOf[JsString].value
      val hue = params.getFields("hue")(0).asInstanceOf[JsString].value
      val sat = params.getFields("sat")(0).asInstanceOf[JsString].value

      this.logger.info(s"[HUE] ControlHueLight: ${on}, ${bri}, ${hue}, ${sat}")
      setLightState(hue.toInt, on, sat.toInt, bri.toInt)
    } catch {
      case e: Exception =>
        logger.warn("setLightState error: " + e.getMessage())
        logger.info(json)
    }
  }

  def setLightState(hue: Int, on: Boolean, sat: Int, bri: Int) {
    val body = s"""
      |{
      |  "hue": ${hue},
      |  "on": ${on},
      |  "sat": ${sat},
      |  "bri": ${bri}
      |}
    """.stripMargin
    this.logger.debug(body)

    val response = putRequest("http://10.100.0.3/api/0F9pNpcZ1mebm0qKP1IlsErWLTJndn7GiRFyjwch/lights/3/state", body)

    logger.info(s"[HUE] Control result >> status: ${response.status}, body: ${response.body}")
    //    val request = HttpRequest(
    //      method = HttpMethods.PUT,
    //      uri = "http://10.100.0.3/api/0F9pNpcZ1mebm0qKP1IlsErWLTJndn7GiRFyjwch/lights/3/state",
    //      //      uri = "http://129.254.169.244:8888/api/252moRHRwqW9KSdqGnppxl44Vxoad7mB5KrHTO9d/lights/3/state",
    //      entity = HttpEntity(ContentTypes.`application/json`, body))
    //
    //    val f = Await.ready(Http().singleRequest(request), 3 seconds)
    //    f.value.get match {
    //      case Success(r) => logger.info(r)
    //      case Failure(e) => logger.error(e)
    //    }

    //    Http().singleRequest(request).onComplete {
    //      case Success(response) => this.logger.info(response)
    //      case Failure(e) => this.logger.error(e)
    //    }
  }

  private def putRequest(url: String, content: String): WSResponse = {
    val CONTENT_TYPE_JSON = ("Content-Type" -> "application/json; charset=utf-8")
    val futureResult: Future[WSResponse] = wsClient.url(url)
      .withHttpHeaders(CONTENT_TYPE_JSON)
      .put(content)
      .map {
        response =>
          response
      }
    Await.result(futureResult, 3 seconds)
  }

  def main(args: Array[String]) {
    while (true) {
      val json = queryParamContext()
      if (json != null) {
        setLightState(json)
        Thread.sleep(5000)
        //        setLightState(0, false, 0, 0)
      }
      //      Thread.sleep(3000)
    }
  }
}
