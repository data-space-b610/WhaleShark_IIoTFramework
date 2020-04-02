package ksb.csle.examples.pipe.tutorial

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto.ParamPair
import ksb.csle.common.utils.ProtoUtils

import ksb.csle.tools.client._

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object ChatbotServingExample extends Logging {
  val appId = this.getClass.getSimpleName.replace("$","")

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    Try (client.submit(
        workflowJson,
        "ksbuser@etri.re.kr",
        this.getClass.getSimpleName.replace("$",""),
        this.getClass.getSimpleName.replace("$",""))) match {
      case Success(id) => logger.info("submit success:" + id)
      case Failure(e) => logger.error("submit error", e)
    }
    client.close()
  }

  private def workflow = {
    val classifierUrl = "http://localhost:18001"
    val chitchatUrl = "http://localhost:18002"
    val defaultUrl = "http://localhost:18888"
    val travelAgencyUrl = "http://localhost:18889"

    val routeQueryClassName = "ksb.csle.component.pipe.query.operator.RouteRestfulContextQueryPipeOperator"
    val routeMapClassName = "ksb.csle.component.pipe.query.operator.RouteMappingPipeOperator"
    val outputQueryClassName = "ksb.csle.component.pipe.query.operator.OutputRestfulContextQueryPipeOperator"

    // Query to model server.
    val operator1 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(routeQueryClassName)
      .setRestQueryOperator(
          RestfulQueryPipeOperatorInfo.newBuilder()
          .setUrl(classifierUrl)
          .setMethod(RestfulQueryPipeOperatorInfo.Method.POST)
          .addHeader(
              ParamPair.newBuilder()
              .setParamName("Content-Type")
              .setParamValue("text/plain; charset=utf-8")
              .build()))

    val operator2 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName(routeMapClassName)
      .setRouteMappingOperator(
          RouteMappingPipeOperatorInfo.newBuilder()
          .addRouteMap(
              RouteMapInfo.newBuilder()
              .setIdx("0")
              .setRoute(chitchatUrl))
          .addRouteMap(
              RouteMapInfo.newBuilder()
              .setIdx("1")
              .setRoute(travelAgencyUrl)))

    val operator3 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName(outputQueryClassName)
      .setRestQueryOperator(
          RestfulQueryPipeOperatorInfo.newBuilder()
          .setUrl(defaultUrl)
          .setMethod(RestfulQueryPipeOperatorInfo.Method.POST)
          .addHeader(
              ParamPair.newBuilder()
              .setParamName("Content-Type")
              .setParamValue("text/plain; charset=utf-8")
              .build()))

    val runner = OnDemandRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.runner.ServingPipeRunner")
      .setWebRunner(
          WebserviceRunnerInfo.newBuilder()
          .setHost("0.0.0.0")
          .setPort(18080))

    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.OnDemandCompositeServingRestfulController")
      .setOnDemandGenericController(
          SimpleOnDemandControllerInfo.getDefaultInstance)

    val multipleQueryEngineInfo = OnDemandPipeServingEngineInfo.newBuilder()
      .setController(controller)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .setRunner(runner)
      .build

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .setVerbose(true)
      .addEngines(
        EngineInfo.newBuilder()
          .setId(1)
          .setPrevId(0)
          .setEngineNickName("ChatbotServingEngine")
          .setOnDemandPipeServingEngine(multipleQueryEngineInfo))
      .build
  }
}
