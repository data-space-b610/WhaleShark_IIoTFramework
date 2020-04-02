package ksb.csle.component.reader

import scala.concurrent._
import scala.concurrent.duration._

import akka.http.scaladsl.model._

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream

import ksb.csle.common.proto.DatasourceProto.{ReaderInfo, StreamReaderInfo}
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.utils.SparkUtils

import ksb.csle.component.ingestion.common._
import ksb.csle.component.ingestion.receiver.HttpPostRequestReceiver

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that receives streaming data via HTTP POST request.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.HttpServerInfo]]
 *          HttpServerInfo contains attributes as follows:
 *          - port: Port number of HTTP server (required)
 *          - ip: IP address of HTTP server (optional)
 *          - path: Path to receive the HTTP POST request (optional)
 *
 * ==HttpServerInfo==
 * {{{
 * message HttpServerInfo {
 *   required int32 port = 1;
 *   optional string ip = 2 [default = "0.0.0.0"];
 *   optional string path = 3 [default = "/"];
 * }
 * }}}
 */
class HttpServerReader(
    val o: StreamReaderInfo
    ) extends BaseReader[
      InputDStream[String], StreamReaderInfo, SparkSession](o) {

  private val info =
    if (o.getHttpServerReader == null) {
      throw new IllegalArgumentException("HttpServerInfo is not set.")
    } else {
      o.getHttpServerReader
    }

  private val handler: HttpIngestionRequest => HttpIngestionResponse = {
    req =>
      // receive any type content.
      HttpIngestionResponse(req.content, StatusCodes.OK, "OK! I got it.")
  }

//  private val handler: HttpIngestionRequest => HttpIngestionResponse = {
//    req =>
//      var data: String = null
//      var statusCode: StatusCode = null
//      var message: String = ""
//
//      req.contentType match {
//        case Some(value) =>
//          value match {
//            case "application/json" =>
//              data = req.content
//              statusCode = StatusCodes.OK
//              message = "OK! I got it."
//            case _ =>
//              statusCode = StatusCodes.BadRequest
//              message = "Oops! only support 'application/json' type."
//          }
//        case None =>
//          statusCode = StatusCodes.BadRequest
//          message = "Oops! Content-Type is not set."
//      }
//
//      HttpIngestionResponse(data, statusCode, message)
//  }

  /**
   * Create an input stream to receives streaming data via HTTP POST request.
   *
   * @param  session              Spark session
   * @return InputDStream[String] Input stream to read HTTP POST request
   */
  @throws(classOf[Exception])
  override def read(session: SparkSession): InputDStream[String] = {
    val ssc = SparkUtils.getStreamingContext(session,
        session.sparkContext.master, 1)

    // if the appName has invalid characters, ActorSystem is not created.
//    val name = if (session.sparkContext.appName.isEmpty()) {
//      "noname_http_" + info.getPort
//    } else {
//      session.sparkContext.appName + "_http_" + info.getPort
//    }
    val name = s"HttpServerReader_${info.getPort}"

    val receiver = new HttpPostRequestReceiver(
        name, info.getIp, info.getPort, handler)

    ssc.receiverStream(receiver)
  }

  /**
   * Close the input stream.
   */
  @throws(classOf[Exception])
  override def close {
    // The HttpPostRequestReceiver instance is terminated automatically,
    // when the Spark Streaming Context is terminated.
  }
}
