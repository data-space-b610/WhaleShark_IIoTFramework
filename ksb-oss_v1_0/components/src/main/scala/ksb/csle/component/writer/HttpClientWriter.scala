package ksb.csle.component.writer

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws.WSRequest
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ahc.AhcWSClient

import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.proto.DatasourceProto.HttpClientInfo
import ksb.csle.common.proto.DatasourceProto.StreamWriterInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Writer that send spark output stream to a http server
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.DatasourceProto.HttpClientInfo]]
 *          HttpClientInfo contains attributes as follows:
 *          - url: http server url includes hostname, port and path (required)
 *          - method: Http request primitives (required)
 *          - hader: http request headers (optional, repeated)
 *          - param: http request params (optional, repeated)
 *          - body: http request body. To be provided when send a post or put request (optional)
 *          - srcColumnName: column to send (optional)
 *
 * == HttpClientInfo ==
 * {{{
 * message HttpClientInfo {
 *   required string url = 1;
 *   required Method method = 2;
 *   enum Method {
 *     POST = 1;
 *     GET = 2;
 *     PUT = 3;
 *     DELETE = 4;
 *   }
 *
 *   repeated HttpParam header = 3;
 *   repeated HttpParam param = 4;
 *   optional string body = 5;
 *   optional string srcColumnName = 6;
 * }
 *
 * message HttpParam {
 *   required string key = 1;
 *   required string value = 2;
 * }
 * }}}
 */
class HttpClientWriter(val o: StreamWriterInfo)
    extends BaseWriter[DataFrame, StreamWriterInfo, SparkSession](o) {

  val REQUEST_TIMEOUT: FiniteDuration = 30 seconds

  private val info =
    if (!o.hasHttpClientWriter()) {
      throw new IllegalArgumentException("HttpClientInfo is not set.")
    } else {
      o.getHttpClientWriter
    }

  private var params = Map[String, String]()
  for (i <- 0 until info.getParamCount) {
    params += (info.getParam(i).getKey -> info.getParam(i).getValue)
  }

  private var headers = Map[String, String]()
  for (i <- 0 until info.getHeaderCount) {
    headers += (info.getHeader(i).getKey -> info.getHeader(i).getValue)
  }

  private val url = info.getUrl
  private val name = "HttpServerWriter_Method_" + info.getMethod.name()

  private val method = info.getMethod

  private implicit val system = ActorSystem(name)
  private implicit val materializer = ActorMaterializer()
  private val wsClient = AhcWSClient()

  /**
   * Operates Write out.
   *
   * @param  df         Input dataframe
   * @return Any  http response message
   */
  override def write(df: DataFrame): Any = {
    val ps = params.toList
    val hs = headers.toList
    val request: WSRequest = wsClient.url(url)
      .addQueryStringParameters(ps: _*)
      .addHttpHeaders(hs: _*)

    info.getSrcColumnName match {
      case null | "" =>
        write(df, request)
      case columnName: String =>
        write(df, request, columnName)
    }
  }

  private def write(inputs: DataFrame, request: WSRequest) {
    inputs.toJSON.collect().foreach { body =>
      logger.info("Request target Url: " + url)
      logger.info("Request body: " + body)

      val rspStatus = method match {
        case HttpClientInfo.Method.POST =>
          postOperation(request, body).statusText
        case HttpClientInfo.Method.PUT  =>
          putOperation(request, body).statusText
        case _ =>
          s"Not supported method; ${method.name()}"
      }
      logger.info(s"Write Result: $rspStatus")
    }
  }

  private def write(inputs: DataFrame, request: WSRequest,
      columnName: String) {
    inputs.select(columnName).collect().foreach { row =>
      val value = row.get(0)
      val body = value match {
        case str: String => str
        case _ => value.toString()
      }

      logger.info("Request target Url: " + url)
      logger.info("Request body: " + body)

      val rspStatus = method match {
        case HttpClientInfo.Method.POST =>
          postOperation(request, body).statusText
        case HttpClientInfo.Method.PUT  =>
          putOperation(request, body).statusText
        case _ =>
          s"Not supported method; ${method.name()}"
      }
      logger.info(s"Write Result: $rspStatus")
    }
  }

  override def close: Any = {
    wsClient.close()
    system.terminate()
  }

  //  private def convertRowToJSON(row: Row): String = {
  //    val m = row.getValuesMap(row.schema.fieldNames)
  //    JSONObject(m).toString()
  //}

  private def postOperation(wsRequest: WSRequest, body: String): WSResponse = {
    val futureResult: Future[WSResponse] = wsRequest
      .withHttpHeaders("Content-Type" -> "text/plain; charset=utf-8")
      .post(body).map {
        response =>
          response
      }
    Await.result(futureResult, REQUEST_TIMEOUT)
  }

  private def putOperation(wsRequest: WSRequest, body: String): WSResponse = {
    val futureResult: Future[WSResponse] = wsRequest
      .withHttpHeaders("Content-Type" -> "text/plain; charset=utf-8")
      .put(body).map {
        response =>
          response
      }
    Await.result(futureResult, REQUEST_TIMEOUT)
  }
}
