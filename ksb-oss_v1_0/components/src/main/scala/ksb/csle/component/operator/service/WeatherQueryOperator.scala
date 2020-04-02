package ksb.csle.component.operator.service

import scala.collection.JavaConversions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.logging.log4j.scala.Logging

import spray.json._
import DefaultJsonProtocol._
import play.api.libs.ws.WSResponse

import ksb.csle.common.proto.OndemandControlProto.OnDemandOperatorInfo
import ksb.csle.common.base.operator.BaseGenericMutantOperator
import ksb.csle.common.utils.ProtoUtils.msgToJson
import ksb.csle.common.utils.response.KbeResponse
import ksb.csle.common.utils.kbe.KbeForwarder

object WeatherJsonProtocol extends DefaultJsonProtocol {
}

/**
 *
 */
class WeatherQueryOperator(
    o: OnDemandOperatorInfo) extends BaseGenericMutantOperator[OnDemandOperatorInfo, Option[String], Option[String]](o) {

  import WeatherJsonProtocol._

  val info = o.getKbeQueryOperatorInfo
  val serverIp = if (info.hasQueryServerIp()) info.getQueryServerIp() else "localhost"
  val serverPort = if (info.hasQureyServerPort()) info.getQureyServerPort() else "9876"
  val serverUrl = s"http://$serverIp:$serverPort"

  override def operate(d: Option[String]): Option[String] = {
    d match {
      case Some(req) => {
        logger.info(s"request[$serverUrl]: $d")
        val result = getQueryResult(req)
        val decoded = result.bodyAsBytes.decodeString("UTF-8")
        Some(decoded)
      }
      case None => Some("Invalid request")
    }
  }

  private def getQueryResult(reqMsg: String): WSResponse = {
    KbeForwarder.forwarderPostJsonRemote("getWeather", reqMsg, serverUrl)
  }

}

object WeatherQueryOperator extends Logging {
  def apply(o: OnDemandOperatorInfo): WeatherQueryOperator = new WeatherQueryOperator(o)
}
