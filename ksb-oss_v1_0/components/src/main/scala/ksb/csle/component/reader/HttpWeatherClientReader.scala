package ksb.csle.component.reader

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.proto.DatasourceProto.HttpWeatherClientInfo.KbClientType
import ksb.csle.common.proto.DatasourceProto.StreamReaderInfo
import ksb.csle.common.utils.SparkUtils
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.kbe.KbeForwarder
import ksb.csle.component.operator.service.HttpWeatherResponseReceiver

/**
 * :: ApplicationDeveloperApi ::
 * A receiver that receives responses from a weather server 
 * provided by Korea Meteorological Administration
 * 
 * @param o Object that contains message
 *          [ksb.csle.common.proto.DatasourceProto.HttpWeatherClientInfo]]
 *          HttpWeatherClientInfo contains attributes as follows:
 *          - kbClientType: weather source type, weather data or storke index data (required)
 *          - location: target city to get the location of weather stations (required)
 *          - getKbServer: Ontology repository server url (optional)
 *          - serviceKey: session key to make a connection with data.go.kr (required)
 *          
 *{{{          
 * message HttpWeatherClientInfo {
 * required KbClientType kbClientType = 1;
 * enum KbClientType {
 *    WEATHER = 1;
 *    STROKEINDEX = 2;
 *  }
 *
 *  required string location = 2 [default = "대전"];
 *  optional string geoKbServer = 3 [default = "http://localhost:9876"];
 *  required string serviceKey = 4;
 *}
 *}}}         
 */
class HttpWeatherClientReader(val o: StreamReaderInfo)
    extends BaseReader[InputDStream[String], StreamReaderInfo, SparkSession](o) with Logging {

  private val info =
    if (!o.hasHttpWeatherReader()) {
      throw new IllegalArgumentException("HttpWeatherClientInfo is not set.")
    } else {
      o.getHttpWeatherReader
    }

  var ssc: StreamingContext = null

  //  val duration = info.getDuration.name()
  val config = ConfigUtils.getConfig()

  val defaultServerIp = config.envOrElseConfig("servers.kbe.serverIp")
  val defaultServerPort = config.envOrElseConfig("servers.kbe.serverPort")
  val defaultServiceKey = 
    "ZJv5%2FHGvy1qy1Uht2JQU%2BUuDoi2ELv2WS1CW83J58d3rOT0xvrkUDq4dvgcOX5P%2Fmi10jpqpJD252SNwaTojRA%3D%3D" //To be acquired from data.go.kr
  val weatherLocationQueryUrl = "getXY"
  val strokeLocationQueryUrl = "getStrokeIndex"

  val kbServerUrl = if (info.hasGeoKbServer()) info.getGeoKbServer 
  else s"http://$defaultServerIp:$defaultServerPort"
  val location = info.getLocation
  val clientType = info.getKbClientType.name()
  val serviceKey = if (info.hasServiceKey()) info.getServiceKey else defaultServiceKey

  @throws(classOf[Exception])
    /**
   * Operates to read from spark memory every 10 seconds.
   *
   * @param  session    Input spark session
   * @return DataFrame  Output dataframe
   */
  override def read(session: SparkSession): InputDStream[String] = {
    ssc = SparkUtils.getStreamingContext(session, session.sparkContext.master, 10)

    val name = if (session.sparkContext.appName.isEmpty()) {
      "default_httpWeatherClient_" + clientType + "_receiver"
    } else {
      session.sparkContext.appName + "_httpWeatherClient_" + clientType + "_receiver"
    }

    val locations = if (info.getKbClientType.equals(KbClientType.WEATHER)) {
      KbeForwarder.forwarderPost(weatherLocationQueryUrl, info.getLocation).body
    } else {
      KbeForwarder.forwarderPost(strokeLocationQueryUrl, info.getLocation).body
    }

    logger.info("retrieved locations: " + locations)

    val receiver = new HttpWeatherResponseReceiver(name, /*duration,*/
      clientType, serviceKey, locations)
    ssc.receiverStream(receiver)
  }

  @throws(classOf[Exception])
    /**
   * Terminate reader
   */
  override def close = {
  }
}
