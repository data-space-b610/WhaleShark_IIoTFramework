package ksb.csle.component.operator.service

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import org.apache.logging.log4j.scala.Logging
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.io.File
import com.typesafe.config.ConfigFactory
import play.libs.Json
import akka.stream.ActorMaterializer
import play.api.libs.ws.ahc.AhcWSClient
import akka.actor.ActorLogging
import akka.actor.PoisonPill
import akka.routing.Broadcast
import play.api.libs.ws.WSResponse
import scala.concurrent.Await
import scala.concurrent.Future
import ksb.csle.component.operator.service._

class WeatherRESTClient(as: ActorSystem, ws: AhcWSClient)
    extends Actor with Logging {

  import scala.concurrent.ExecutionContext.Implicits._
  import scala.concurrent.duration._

  val RESPONSE_TIMEOUT = 30 seconds

  def receive = {
    case wr: WeatherRequest ⇒ {
      sender ! getWeatherData(wr)
    }
    case sr: StrokeIndexRequest ⇒ {
      sender ! getStrokeIndexData(sr)
    }
    case _ ⇒ logger.info("Not supported weather location...")
  }

  private def getWeatherData(wr: WeatherRequest): String = {
    // time format: yyyymmddHHMM
    val url = wr.baseUrl + "?ServiceKey=" +
      wr.serviceKey + "&base_date=" + wr.time +
      "&nx=" + wr.nx + "&ny=" + wr.ny + "&_type=" + "json"
      logger.info("**** url: " + url)
    val result = getOperation(ws, url)
    val jv = Json.parse(result.body)
    val status = jv.findValue("resultCode")
    val returnVal = if (status.asText().equals("0000")) result.body else "Not_Found"
    returnVal.asInstanceOf[String]
  }

  private def getStrokeIndexData(sr: StrokeIndexRequest): String = {
    // time format: yyyyMMddHH
    val url = sr.baseUrl + "?ServiceKey=" +
      sr.serviceKey + "&areaNo=" + sr.coord +
      "&time=" + sr.time + "&_type=" + "json"
      logger.info("**** url: " + url)
    val result = getOperation(ws, url)
    val jv = Json.parse(result.body)
    val status = jv.findValue("returnCode")
    val returnVal = if (status.asText().equals("00")) result.body else "Not_Found"
    returnVal.asInstanceOf[String]
  }

  private def getOperation(ws: AhcWSClient, url: String): WSResponse = {
    val futureResult: Future[WSResponse] = ws.url(url).get.map {
      response =>
        response
    }
    Await.result(futureResult, RESPONSE_TIMEOUT)
  }
}
