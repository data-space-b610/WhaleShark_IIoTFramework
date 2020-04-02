package ksb.csle.component.pipe.query.operator

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging

import akka.actor.{ Actor, ActorLogging, ActorSystem }
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.http.scaladsl.model._
import play.api.libs.ws.ahc.AhcWSClient
import play.api.libs.ws.{ WSRequest, WSResponse }
import spray.json._

import ksb.csle.common.base.param.BaseRouteContext
import ksb.csle.common.base.pipe.query.operator.BaseContextQueryPipeOperator
import ksb.csle.common.proto.OndemandControlProto.OnDemandPipeOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.RestfulQueryPipeOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.RestfulQueryPipeOperatorInfo.Method._

/**
 * Operator that queries to rest server when it is requested
 */
class GenericRestfulContextQueryPipeOperator[T](
    override val p: OnDemandPipeOperatorInfo,
    override implicit val system: ActorSystem
    ) extends BaseContextQueryPipeOperator[BaseRouteContext[T], ActorSystem](p, system) with Logging {

  private implicit val materializer = ActorMaterializer()
  private val wsClient = AhcWSClient()
  private val REQUEST_TIMEOUT: FiniteDuration = 30 seconds

  protected val info: RestfulQueryPipeOperatorInfo = p.getOneofOperatorsCase match {
    case OnDemandPipeOperatorInfo.OneofOperatorsCase.RESTDATAQUERYOPERATOR =>
      p.getRestDataQueryOperator
    case OnDemandPipeOperatorInfo.OneofOperatorsCase.RESTROUTEQUERYOPERATOR =>
      p.getRestRouteQueryOperator
    case OnDemandPipeOperatorInfo.OneofOperatorsCase.RESTQUERYOPERATOR =>
      p.getRestQueryOperator
    case OnDemandPipeOperatorInfo.OneofOperatorsCase.OUTPUTCUMULATIVEOPERATOR =>
      p.getOutputCumulativeOperator
    case _ => p.getRestQueryOperator
  }

  private def params = info.getParamList.map(param => (param.getParamName -> param.getParamValue)).toMap
  private def headers = info.getHeaderList.map(header => (header.getParamName -> header.getParamValue)).toMap

  private def wsRequest: WSRequest = wsClient.url(info.getUrl)
    .addQueryStringParameters(params.toList: _*)
    .addHttpHeaders(headers.toList: _*)
    .withHttpHeaders("Content-Type" -> "application/json; charset=utf-8")

  private def query(body: String): Future[WSResponse] = info.getMethod match {
    case POST | PUT => wsRequest.post(body)
    case GET => wsRequest.get
    case DELETE => wsRequest.delete
  }

  // TODO: Split dynamic restfulQueryOperator.
  protected def query(url: String, body: String): Future[WSResponse] = {
    val wsRequest = wsClient.url(url)
    info.getMethod match {
      case POST | PUT => wsRequest.post(body)
      case GET => wsRequest.get
      case DELETE => wsRequest.delete
    }
  }

  protected def queryUrl(url: String, ctx: BaseRouteContext[T]): BaseRouteContext[T] = {
    logger.debug(s"Query opId #${p.getId} called !!")
    logger.debug(s"Query to url ${url} data ${ctx.toJsonString} called !!")
    val result = Await.result(
        query(url, ctx.getData().getOrElse(ctx.getInput().getOrElse("")))
        .map(response => response), REQUEST_TIMEOUT)
    result.status match {
      case 200 | 201 =>
        if (result.body.isEmpty) {
          logger.info("Query: body is empty!")
          ctx
        } else {
          ctx.setOutput(Some(result.body.replace("\'", "").replace("\"","").replace("\n","")))
          ctx
        }
      case _ =>
        logger.info("Query: failed, " + result.body)
        ctx
    }
  }

  override def query(ctx: BaseRouteContext[T]): BaseRouteContext[T] = {
    logger.debug(s"Query opId #${p.getId} called !!")
    logger.debug(s"Data for query opId #${p.getId} : ${ctx.toJsonString}")

    val result = Await.result(
        query(ctx.getData().getOrElse(ctx.getInput().getOrElse("")))
        .map(response => response), REQUEST_TIMEOUT)
    result.status match {
      case 200 | 201 =>
        if (result.body.isEmpty) {
          logger.info("Query: body is empty!")
          ctx
        } else {
          logger.debug("result.body --> " + result.body)
          ctx.setOutput(Some(result.body.replace("\'", "").replace("\"","").replace("\n","")))
          ctx
        }
      case _ =>
        logger.info("Query: failed, " + result.body)
        ctx
    }
  }

  override def stop = {
    wsClient.close()
    system.terminate()
  }
}
