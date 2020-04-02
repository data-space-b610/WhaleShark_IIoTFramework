package ksb.csle.examples.device

import java.util
import scala.collection.JavaConverters._

import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.{ HttpGet, HttpPut, HttpPost }
import org.apache.http.impl.client.HttpClients
import org.apache.http.{ HttpEntity, HttpStatus }
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.HttpHeaders
import org.apache.logging.log4j.scala.Logging
import org.apache.kafka.clients.consumer.KafkaConsumer

import spray.json._
//import play.api.libs.json.{JsArray, JsValue, Json, Writes}

object HUEControlSample extends Logging {

  def makeRuleJson(json: String): String = {
    val params: JsObject = json.parseJson.asJsObject
    val location = params.getFields("location")(0).asInstanceOf[JsString].value
    val time = System.currentTimeMillis().toString()
    val existance =
      (params.getFields("avg(existance)")(0).asInstanceOf[JsNumber].value.toInt > 0)
    val ruleJson = s"""{"location":"${location}", "time":"${time}", "existance":${existance}}"""

    //      println(ruleJson)
    ruleJson
  }

  def main(args: Array[String]) {
    val TOPIC = "topicMldlFinal"
    val consumer: KafkaConsumer[String, String] = new HueConsumer().get()
    consumer.subscribe(util.Collections.singletonList(TOPIC))

    val serviceApi = new ServicePostApi("http://0.0.0.0:8765/predict")
    val hueApi = new HUEApi("10.100.0.2", "rbi5TEX5a-LQi7EBtiq7S2Q9kA0w2SviJuxf15hy", "3")

    while (true) {
      consumer.poll(100).asScala.map(_.value()).foreach { e =>
        val json = makeRuleJson(e)
        val (statusCode, content, error) = serviceApi.queryParamContext(json)
        if (statusCode == HttpStatus.SC_OK) {
          content match {
            case Some(json) =>
              this.logger.info(s"paramContext:\n${json}")
              controlHueLight(hueApi, json)
              Thread.sleep(1000)
            //            hueApi.lightOff()
            case None =>
              this.logger.warn("no content")
          }
        }
      }
      Thread.sleep(1000)
    }
  }

  private def controlHueLight(hueApi: HUEApi, json: String) {
    val msg_format = """
[
    {
        "thingId": "S_HUE_light_3",
        "resourceId": "controller",
        "context": [
            "HotContext"
        ],
        "service": "hue_turnon_Blue",
        "controls": {
            "sat": "254",
            "hue": "46920",
            "bri": "254",
            "on": "true"
        }
    }
]
"""
    
    try {
      //      val params = json.parseJson.asJsObject
      //      val params = root.getFields("controls")(0).asJsObject
      //      val on = params.getFields("on")(0).asInstanceOf[JsBoolean].value
      //      val bri = params.getFields("bri")(0).asInstanceOf[JsNumber].value
      //      val hue = params.getFields("hue")(0).asInstanceOf[JsNumber].value
      //      val sat = params.getFields("sat")(0).asInstanceOf[JsNumber].value
      //
      //      val params = json.parseJson.asJsObject.getFields("params")(0).asJsObject
      //      val on = params.getFields("on")(0).asInstanceOf[JsString].value
      //      val bri = params.getFields("bri")(0).asInstanceOf[JsString].value
      //      val hue = params.getFields("hue")(0).asInstanceOf[JsString].value
      //      val sat = params.getFields("sat")(0).asInstanceOf[JsString].value

      val originalMsg = json.parseJson.asInstanceOf[JsArray]
      val parsedMsg = originalMsg.elements.head.asJsObject
      val params = parsedMsg.getFields("controls")(0).asJsObject
      val on = params.getFields("on")(0).asInstanceOf[JsString].value.toBoolean
      val bri = params.getFields("bri")(0).asInstanceOf[JsString].value
      val hue = params.getFields("hue")(0).asInstanceOf[JsString].value
      val sat = params.getFields("sat")(0).asInstanceOf[JsString].value

      this.logger.info(s"controlHueLight: ${on}, ${bri}, ${hue}, ${sat}")

      val (statusCode, error) = hueApi.setLightState(
        on, bri.toInt, hue.toInt, sat.toInt)

      this.logger.info(s"controlHueLight: ${statusCode}, ${error}")
    } catch {
      case e: Exception => this.logger.info(
        s"controlHueLight error: ${e.getMessage}")
    }
  }
}

class ServiceApi(uri: String) {
  def queryParamContext(): (Int, Option[String], Option[String]) = {
    val httpGet = new HttpGet(uri);
    val client = HttpClients.createDefault();
    try {
      val response = client.execute(httpGet);
      if (response == null) {
        (HttpStatus.SC_REQUEST_TIMEOUT, None, None)
      } else {
        var content: Option[String] = None
        var statusCode = response.getStatusLine.getStatusCode
        val entity = response.getEntity();
        if (entity != null) {
          val body = EntityUtils.toString(entity)
          if (body == null) {
            content = None
          } else {
            content = Some(body)
          }
        }
        response.close();
        (statusCode, content, None)
      }
    } catch {
      case e: Exception =>
        (HttpStatus.SC_REQUEST_TIMEOUT, None, Some(e.getMessage))
    } finally {
      client.close();
    }
  }
}

case class EnergyControl(timestamp: String, location: String, Sensored: Boolean)
case class HueControls(hue: Int, on: Boolean, bri: Int, time: String, light: String, sat: Int)
case class DeviceContextControlCase(
  thingId: String, resourceId: String, ownerId: String, context: String, controls: HueControls)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val energyControlFormat = jsonFormat3(EnergyControl)
  implicit val HueControlsFormat = jsonFormat6(HueControls)
  implicit val DeviceContextControlCaseFormat = jsonFormat5(DeviceContextControlCase)
}

import MyJsonProtocol._
import scala.util.parsing.json.JSONArray
import scala.util.parsing.json.JSONArray
import scala.util.parsing.json.JSONArray
import play.api.libs.json.Json
import play.api.libs.json.Json
import play.api.libs.json.Json
import play.libs.Json

class ServicePostApi(uri: String) {
  def queryParamContext(json: String): (Int, Option[String], Option[String]) = {

    //    val json = """{"timestamp" : "2018-04-27T08:34:33", "location" : "C1B1Z1R1", "Sensored" : true}"""
    //    val json = """{"location":"C1B1Z1R1", "time":"2018-04-24T04:34:33", "existance":true}"""
    //    val content_ = """    {
    //        "thingId": "S_HUE_light_3",
    //        "resourceId": "controller",
    //        "context": [
    //            "ColdContext"
    //        ],
    //        "service": "hue_turnon_Red",
    //        "controls": {
    //            "sat": "254",
    //            "hue": "65535",
    //            "bri": "254",
    //            "on": "true"
    //        }
    //    }"""
    //    val caseObject = json.parseJson.convertTo[EnergyControl]
    val post = new HttpPost(uri)
    post.setHeader(HttpHeaders.ACCEPT, "application/json")
    post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    post.setEntity(new StringEntity(json))

    val client = HttpClients.createDefault()
    try {
      val response = client.execute(post)
      if (response == null) {
        (HttpStatus.SC_REQUEST_TIMEOUT, None, None)
      } else {
        var content: Option[String] = None
        var statusCode = response.getStatusLine.getStatusCode
        val entity = response.getEntity();
        if (entity != null) {
          val body = EntityUtils.toString(entity)
          if (body == null) {
            content = None
          } else {
            content = Some(body)
          }
        }
        response.close();
        //        content = Some(content_)
        //        content = Some(s"""{"controls": ${content.get}}""")
        (statusCode, content, None)
      }
    } catch {
      case e: Exception =>
        (HttpStatus.SC_REQUEST_TIMEOUT, None, Some(e.getMessage))
    } finally {
      client.close();
    }
  }
}

class HUEApi(bridgeAddr: String, userName: String, lightId: String) {
  private val URI = s"http://${bridgeAddr}/api/${userName}/lights/${lightId}/state"

  def lightOn(): (Int, Option[String]) = setLightState(true)

  def lightOff(): (Int, Option[String]) = setLightState(false)

  def setLightState(on: Boolean): (Int, Option[String]) = {
    val body = s"""
        |{
        |  "on": ${on}
        |}
      """.stripMargin

    setLightState(body)
  }

  def setLightState(on: Boolean, bri: Int, hue: Int, sat: Int): (Int, Option[String]) = {
    val body = s"""
        |{
        |  "on": ${on},
        |  "bri": ${bri},
        |  "hue": ${hue},
        |  "sat": ${sat}
        |}
      """.stripMargin

    setLightState(body)
  }

  def setLightState(body: String): (Int, Option[String]) = {
    val httpPut = new HttpPut(URI);
    val client = HttpClients.createDefault();
    try {
      httpPut.setEntity(new StringEntity(body));
      val response = client.execute(httpPut);
      if (response == null) {
        (HttpStatus.SC_REQUEST_TIMEOUT, None)
      } else {
        (response.getStatusLine().getStatusCode(), None)
      }
    } catch {
      case e: Exception =>
        (HttpStatus.SC_REQUEST_TIMEOUT, Some(e.getMessage))
    } finally {
      client.close();
    }
  }
}
