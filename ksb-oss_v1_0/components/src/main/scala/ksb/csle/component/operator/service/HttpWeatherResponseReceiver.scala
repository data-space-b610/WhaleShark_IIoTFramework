package ksb.csle.component.operator.service

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Future

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.StreamingContext

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.util.ByteString
import akka.routing.RoundRobinPool
import akka.actor.SupervisorStrategy
import akka.actor.Props
import akka.util.Timeout
import akka.pattern.{ ask, gracefulStop, pipe }
import play.api.libs.ws.ahc.AhcWSClient
import play.api.libs.ws.{ WSRequest, WSResponse }
import play.libs.Json

import ksb.csle.common.utils.kbe.KbeUtils
import ksb.csle.common.base.controller.BaseKnowledgeCumulator
import ksb.csle.common.proto.DatasourceProto.HttpWeatherClientInfo.KbClientType

final class HttpWeatherResponseReceiver(val name: String, /*val duration: String,*/
                                        val clientType: String, val serviceKey: String, val locations: String)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  val RESPONSE_TIMEOUT = 30 seconds
  val CLIENT_SLEEP_TIME = 60000

  case class StopArgs(system: ActorSystem, wsClient: AhcWSClient)

  val serviceUrl: String =
    if (clientType.equals(KbClientType.WEATHER.name()))
      KbeUtils.getWeatherServiceBaseUrl()
    else KbeUtils.getStrokeServiceBaseUrl()

  var stopArgs: StopArgs = null

  override def onStart(): Unit = {
    stopArgs = receive()
  }

  override def onStop(): Unit = {
    if (stopArgs != null) {
      stop(stopArgs)
    }
  }

  private def receive(): StopArgs = {

    import scala.concurrent.ExecutionContext.Implicits._
    import scala.concurrent.duration._

    implicit val system = ActorSystem(name)
    implicit val materializer = ActorMaterializer()
    val wsClient = AhcWSClient()

    val loc = Json.parse(locations)

    var t = 0

    val client = system.actorOf(RoundRobinPool(loc.size() + 1,
      supervisorStrategy = SupervisorStrategy.defaultStrategy)
      .props(Props(new WeatherRESTClient(system, wsClient))),
      name = "WeatherRESTClient")

    clientType match {
      case "WEATHER" => {

        var prevHr: Int = 0

        while (true) {

          val date: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
          val hour: SimpleDateFormat = new SimpleDateFormat("HH")
          val min: SimpleDateFormat = new SimpleDateFormat("mm")
          val c1: Calendar = Calendar.getInstance
          val cal: Date = c1.getTime
          var strMin: String = min.format(cal)
          val curMin: Int = strMin.toInt

          if (curMin >= 0 && curMin <= 40) {
            c1.add(Calendar.HOUR, -1)
            strMin = "00"
          } else if (curMin > 40 && curMin <= 59) {
            strMin = "00"
          } else {
            strMin = strMin
          }

          val now: Date = c1.getTime
          val strDate: String = date.format(now)
          val strHr: String = hour.format(now)
          val curHr: Int = strHr.toInt
          val time = strDate + strHr + strMin

          if (prevHr == curHr) {
            // do nothing
            logger.info(
              "[" + t + "] RequestTime: " + time + " >> Do nothing: 1 time per 1 hour, CurHr: " + curHr + ", PrevHr: " + prevHr)
          } else {
            for (i <- 0 until loc.size()) {
              implicit val timeout = new Timeout(RESPONSE_TIMEOUT)
              val future: Future[String] = ask(client, WeatherRequest(serviceUrl,
                serviceKey, time, loc.get(i).get("x").asText(),
                loc.get(i).get("y").asText())).mapTo[String]
              val result = Await.result(future, RESPONSE_TIMEOUT)
              logger.info("[" + t + "] RequestTime: " + time + " >> Result: " + result)
              if (!result.equals("Not_Found")) {
                store(result)
                logger.info("Store OK")
              } else logger.warn("Store failed: " + result)

            }
            t += 1
            prevHr = curHr
          }
          Thread.sleep(CLIENT_SLEEP_TIME)
        }
      }

      case "STROKEINDEX" => {

        var amFlag = false
        var pmFlag = false
        var noFlag = false
        var isInitial = true

        while (true) {

          val date = new SimpleDateFormat("yyyyMMdd")
          val hour = new SimpleDateFormat("HH")
          val c1 = Calendar.getInstance()
          val now = c1.getTime()
          val strToday = date.format(now)
          val strHr = hour.format(now)
          val numHr = strHr.toInt
          val time = strToday + strHr

          if (isInitial) {
            for (i <- 0 until loc.size()) {
              implicit val timeout = new Timeout(RESPONSE_TIMEOUT)
              val future: Future[String] = ask(client, StrokeIndexRequest(serviceUrl,
                serviceKey, time, loc.get(i).get("coord").asText())).mapTo[String]
              val result = Await.result(future, RESPONSE_TIMEOUT)
              logger.info("[" + t + "] RequestTime: " + time + " >> Result: " + result)
              if (!result.equals("Not_Found")) store(result)
              logger.info("Store OK")
            }
            isInitial = false
            t += 1
          } else if (numHr < 6) {
            // do nothing
            logger.info("[" + t + "] Do nothing, cause >> amFlag: " + amFlag + ", pmFlag: " + pmFlag)
          } else if (amFlag == false && numHr == 6) { // morning
            for (i <- 0 until loc.size()) {
              implicit val timeout = new Timeout(RESPONSE_TIMEOUT)
              val future: Future[String] = ask(client, StrokeIndexRequest(serviceUrl,
                serviceKey, time, loc.get(i).get("coord").asText())).mapTo[String]
              val result = Await.result(future, RESPONSE_TIMEOUT)
              logger.info("[" + t + "] RequestTime: " + time + " >> Result: " + result)
              if (!result.equals("Not_Found")) {
                store(result)
                logger.info("Store OK")
              } else logger.warn("Store failed: " + result)

            }
            amFlag = true
            pmFlag = false
            t += 1
          } else if (pmFlag == false && numHr == 18) { // afternoon
            for (i <- 0 until loc.size()) {
              implicit val timeout = new Timeout(RESPONSE_TIMEOUT)
              val future: Future[String] = ask(client, StrokeIndexRequest(serviceUrl,
                serviceKey, time, loc.get(i).get("coord").asText())).mapTo[String]
              val result = Await.result(future, RESPONSE_TIMEOUT)
              logger.info("[" + t + "] RequestTime: " + time + " >> Result: " + result)
              if (!result.equals("Not_Found")) {
                store(result)
                logger.info("Store OK")
              } else logger.warn("Store failed: " + result)

            }
            pmFlag = true
            amFlag = false
            t += 1
          } else {
            // do nothing
            logger.info("[" + t + "] Do nothing, cause >> amFlag: " + amFlag + ", pmFlag: " + pmFlag)
          }
          Thread.sleep(CLIENT_SLEEP_TIME)
        }
      }
      case _ => logger.warn("Not supported weather client type: " + clientType)
    }
    StopArgs(system, wsClient)
  }

  private def stop(stopArgs: StopArgs) = {
    stopArgs.wsClient.close()
    stopArgs.system.terminate()
  }

}
