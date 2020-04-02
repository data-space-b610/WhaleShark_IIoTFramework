package ksb.csle.component.reader

import akka.http.scaladsl.model._

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream

import ksb.csle.common.proto.DatasourceProto.{ReaderInfo, StreamReaderInfo}
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.utils.SparkUtils

import ksb.csle.component.ingestion.common._
import ksb.csle.component.ingestion.util.OneM2MUtils
import ksb.csle.component.ingestion.receiver.HttpPostRequestReceiver

/**
 * :: ApplicationDeveloperApi ::
 *
 * Reader that receives streaming data via oneM2M HTTP binding.
 * (Note: In the current version, assumes that container resource is already
 *  subscribed by the external system.)
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.OneM2MInfo]]
 *          OneM2MInfo contains attributes as follows:
 *          - cseBase: URI of CSEBase resource (TDB, set to empty string)
 *          - originatorID: ID of subscriber (TDB, set to empty string)
 *          - resourceURI: URIs of resources to subscribe (TDB, set to empty list)
 *          - http: HTTP binding attributes, see 'OneM2MHttpInfo' (optional)
 *
 *  ==OneM2MInfo==
 * {{{
 * message OneM2MInfo {
 *   required string cseBase = 1;
 *   required string originatorID = 2;
 *   repeated string resourceURI = 3;
 *   oneof oneof_protocols {
 *     OneM2MHttpInfo http = 4;
 *     OneM2MMqttInfo mqtt = 5;
 *   }
 * }
 * }}}
 *
 *          [[ksb.csle.common.proto.DatasourceProto.OneM2MHttpInfo]]
 *          OneM2MHttpInfo contains attributes as follows:
 *          - cseAddress: IP address of oneM2M CSE  (TDB, set to empty string)
 *          - csePort: Port number of oneM2M CSE (TDB, set to any integer)
 *          - subscriberPort: Port number of HTTP server to receive
 *                            oneM2M notification (required)
 *          - subscriberAddress: IP address of HTTP server (optional)
 *
 * ==OneM2MHttpInfo==
 * {{{
 * message OneM2MHttpInfo {
 *   required string cseAddress = 1;
 *   required int32 csePort = 2;
 *   required int32 subscriberPort = 3;
 *   optional string subscriberAddress = 4 [default = "0.0.0.0"];
 * }
 * }}}
 */
class Onem2mHttpReader(
    val o: StreamReaderInfo
    ) extends BaseReader[
      InputDStream[String], StreamReaderInfo, SparkSession](o) {

  private val info =
    if (o.getOneM2MReader == null) {
      throw new IllegalArgumentException("OneM2MInfo is not set.")
    } else {
      if (o.getOneM2MReader.getHttp == null) {
        throw new IllegalArgumentException("HTTP Info of oneM2M is not set.")
      } else {
        o.getOneM2MReader
      }
    }

  private val handler: HttpIngestionRequest => HttpIngestionResponse = {
    req =>
      var data: String = null
      var statusCode: StatusCode = null
      var message: String = ""

      req.contentType match {
        case Some(value) =>
          value match {
            case "application/json" | "application/vnd.onem2m-ntfy+json" =>
              data = OneM2MUtils.extractContent(req.content, value)
              statusCode = StatusCodes.OK
              message = "OK! I got it."
            case _ =>
              statusCode = StatusCodes.BadRequest
              message = "Oops! only support 'application/json' type."
          }
        case None =>
          statusCode = StatusCodes.BadRequest
          message = "Oops! Content-Type is not set."
      }

      HttpIngestionResponse(data, statusCode, message)
  }

  /**
   * Create an input stream to receives streaming data via oneM2M HTTP binding.
   *
   * @param  session              Spark session
   * @return InputDStream[String] Input stream to read oneM2M notification.
   */
  @throws(classOf[Exception])
  override def read(session: SparkSession): InputDStream[String] = {
    val ssc = SparkUtils.getStreamingContext(session,
        session.sparkContext.master, 1)

    val name = s"Onem2mHttpReader_${info.getHttp.getSubscriberPort}"

    val receiver = new HttpPostRequestReceiver(name,
        info.getHttp.getSubscriberAddress,
        info.getHttp.getSubscriberPort,
        handler)

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
