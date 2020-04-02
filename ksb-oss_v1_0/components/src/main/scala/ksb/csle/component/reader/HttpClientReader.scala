package ksb.csle.component.reader

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.proto.DatasourceProto.HttpClientInfo
import ksb.csle.common.proto.DatasourceProto.StreamReaderInfo
import ksb.csle.common.utils.SparkUtils
import ksb.csle.component.ingestion.receiver.HttpDeleteResponseReceiver
import ksb.csle.component.ingestion.receiver.HttpGetResponseReceiver
import ksb.csle.component.ingestion.receiver.HttpPostResponseReceiver
import ksb.csle.component.ingestion.receiver.HttpPutResponseReceiver

/**
 * :: ApplicationDeveloperApi ::
 * A receiver that receives responses from a http server through
 * http methods and store it in spark memory
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.HttpClientInfo]]
 *          HttpClientInfo contains attributes as follows:
 *          - url: http server url includes hostname, port and path (required)
 *          - method: Http request primitives (required)
 *          - hader: http request headers (optional, repeated)
 *          - param: http request params (optional, repeated)
 *          - body: http request body. To be provided when send a post or put request (optional)
 *
 * == HttpClientInfo ==
 * {{{
 * message HttpClientInfo {
 *  required string url = 1;
 *  required Method method = 2;
 *  enum Method {
 *    POST = 1;
 *    GET = 2;
 *    PUT = 3;
 *    DELETE = 4;
 * }
 *
 * repeated HttpParam header = 3;
 * repeated HttpParam param = 4;
 * optional string body = 5;
 * }
 *
 * message HttpParam {
 * required string key = 1;
 * required string value = 2;
 * }
 * }}}
 *
 */
class HttpClientReader(val o: StreamReaderInfo)
    extends BaseReader[InputDStream[String], StreamReaderInfo, SparkSession](o) {

  private val info = o.getHttpReader
  private val url = info.getUrl
  private val method = info.getMethod
  private val body = info.getBody

  var ssc: StreamingContext = null

  @throws(classOf[Exception])
  override def read(session: SparkSession): InputDStream[String] = {
    ssc = SparkUtils.getStreamingContext(session, session.sparkContext.master, 2)
    var result: ReceiverInputDStream[String] = null

    val name = if (session.sparkContext.appName.isEmpty()) {
      "default_httpClient_" + method
    } else {
      session.sparkContext.appName + "httpClient_" + method
    }
    val params = info.getParamList.map(param => (param.getKey -> param.getValue)).toMap
    val headers = info.getHeaderList.map(header => (header.getKey -> header.getValue)).toMap
    val receiver: Receiver[String] = info.getMethod match {
      case HttpClientInfo.Method.POST   => new HttpPostResponseReceiver(name, url, params, headers, body)
      case HttpClientInfo.Method.GET    => new HttpGetResponseReceiver(name, url, params, headers)
      case HttpClientInfo.Method.PUT    => new HttpPutResponseReceiver(name, url, params, headers, body)
      case HttpClientInfo.Method.DELETE => new HttpDeleteResponseReceiver(name, url, params, headers)
    }
    ssc.receiverStream(receiver)
  }

  @throws(classOf[Exception])
  override def close = {
  }
}
