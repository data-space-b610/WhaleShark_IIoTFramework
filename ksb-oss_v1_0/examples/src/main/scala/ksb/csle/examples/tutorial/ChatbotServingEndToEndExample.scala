package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.OndemandOperatorProto.TensorflowServingOperatorInfo
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
object ChatbotServingEndToEndExample extends Logging {
  private val appId = this.getClass.getSimpleName.replace("$","")
  private val host = "192.168.0.5"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("0.0.0.0", 19999)
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

  private def classfierModelServingEngine = {
    val port = 8001
    val modelName = "classify-tf"
    val modelBasePath =
      "model/classify-tf"

    val runner = OnDemandRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.TensorflowServingRunner")
      .setTfServingRunner(
          TensorflowServingRunnerInfo.newBuilder()
            .setPort(port)
            .setModelName(modelName)
            .setModelBasePath(modelBasePath))
      .build()

    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.TensorflowServingController")
      .setTensorflowServingController(SimpleOnDemandControllerInfo.getDefaultInstance)

    val predictOper = OnDemandOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.service.TensorflowServingOperator")
      .setTensorServingOperator(
          TensorflowServingOperatorInfo.getDefaultInstance())
      .build()

    val nginInfo = OnDemandServingEngineInfo.newBuilder()
      .setController(controller)
      .setRunner(runner)
      .setOperator(predictOper)
      .build

    nginInfo
  }

  private def chitchatterModelServingEngine = {
    val port = 8002
    val modelName = "chitchat-tf"
    val modelBasePath = "model/chitchat-tf"

    val runner = OnDemandRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.TensorflowServingRunner")
      .setTfServingRunner(
          TensorflowServingRunnerInfo.newBuilder()
            .setPort(port)
            .setModelName(modelName)
            .setModelBasePath(modelBasePath))
      .build()

    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.TensorflowServingController")
      .setTensorflowServingController(SimpleOnDemandControllerInfo.getDefaultInstance)

    val predictOper = OnDemandOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.service.TensorflowServingOperator")
      .setTensorServingOperator(
          TensorflowServingOperatorInfo.getDefaultInstance())
      .build()

    val nginInfo = OnDemandServingEngineInfo.newBuilder()
      .setController(controller)
      .setRunner(runner)
      .setOperator(predictOper)
      .build

    nginInfo
  }

  private def classificationPreprocessEngine = {
    val runner = OnDemandExternalRunnerInfo.newBuilder()
      .setClsName("dummyClass")
      .setPyContainerRunner(
          PyContainerRunnerInfo.newBuilder()
            .setPort(18001)
            .setImgName("classify_image")
            .setCodePath(s"file:///${ksbHome}/examples/pyModules/ChatbotServing/classify")
            .setLibraries("nltk:numpy")
            .setUserArgs(s"http://${host}:8001/model"))
      .build()

    val nginInfo = OnDemandExternalEngineInfo.newBuilder()
      .setRunner(runner)
      .build

    nginInfo
  }

  private def chitchatPreprocessEngine = {
    val runner = OnDemandExternalRunnerInfo.newBuilder()
      .setClsName("dummyClass")
      .setPyContainerRunner(
          PyContainerRunnerInfo.newBuilder()
            .setPort(18002)
            .setImgName("chitchat_image")
            .setCodePath(s"file:///${ksbHome}/examples/pyModules/ChatbotServing/chitchat")
            .setLibraries("nltk:numpy")
            .setUserArgs(s"http://${host}:8002/model"))
      .build()

    val nginInfo = OnDemandExternalEngineInfo.newBuilder()
      .setRunner(runner)
      .build

    nginInfo
  }

  private def travelAgencyEngine = {
    val runner = OnDemandExternalRunnerInfo.newBuilder()
      .setClsName("dummyClass")
      .setPyContainerRunner(
          PyContainerRunnerInfo.newBuilder()
            .setPort(18003)
            .setImgName("travel_image")
            .setCodePath(s"file:///${ksbHome}/examples/pyModules/ChatbotServing/travel"))
      .build()

    val nginInfo = OnDemandExternalEngineInfo.newBuilder()
      .setRunner(runner)
      .build

    nginInfo
  }

  private def chatbotServingEngine = {
    val classifierUrl = s"http://${host}:18001"
    val chitchatUrl = s"http://${host}:18002"
    val defaultUrl = s"http://${host}:18888"
    val travelAgencyUrl = s"http://${host}:18003"

    val routeQueryClassName = "ksb.csle.component.pipe.query.operator.RouteRestfulContextQueryPipeOperator"
    val routeMapClassName = "ksb.csle.component.pipe.query.operator.RouteMappingPipeOperator"
    val outputQueryClassName = "ksb.csle.component.pipe.query.operator.OutputRestfulContextQueryPipeOperator"

    // Query to model server.
    val operator1 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(routeQueryClassName)
      .setRestRouteQueryOperator(
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
      .setRestOutputQueryOperator(
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
      .setWebPipeRunner(
          WebserviceRunnerInfo.newBuilder()
          .setHost("0.0.0.0")
          .setPort(18080))

    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.OnDemandCompositeServingRestfulController")
      .setOnDemandCompositeServingController(
          SimpleOnDemandControllerInfo.getDefaultInstance)

    OnDemandPipeServingEngineInfo.newBuilder()
      .setController(controller)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .setRunner(runner)
      .build
  }

  private def workflow = {

    val runType1 = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()
    val runType2 = RunType.newBuilder()
      .setId(2)
      .setPeriodic(Periodic.ONCE)
      .build()
    val runType3 = RunType.newBuilder()
      .setId(3)
      .setPeriodic(Periodic.ONCE)
      .build()
    val runType4 = RunType.newBuilder()
      .setId(4)
      .setPeriodic(Periodic.ONCE)
      .build()
    val runType5 = RunType.newBuilder()
      .setId(5)
      .setPeriodic(Periodic.ONCE)
      .build()
    val runType6 = RunType.newBuilder()
      .setId(6)
      .setPeriodic(Periodic.ONCE)
      .build()

    WorkflowInfo.newBuilder()
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType1)
      .addRuntypes(runType2)
      .addRuntypes(runType3)
      .addRuntypes(runType4)
      .addRuntypes(runType5)
      .addRuntypes(runType6)
      .addEngines(
        EngineInfo.newBuilder()
          .setId(1)
          .setPrevId(0)
          .setEngineNickName("ClassfierModelServingEngine")
          .setOnDemandServingEngine(classfierModelServingEngine))
      .addEngines(
        EngineInfo.newBuilder()
          .setId(2)
          .setPrevId(1)
          .setEngineNickName("ChitchatterModelServingEngine")
          .setOnDemandServingEngine(chitchatterModelServingEngine))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(3)
            .setPrevId(2)
            .setEngineNickName("ClassificationServingEngine")
            .setOnDemandExternalServingEngine(classificationPreprocessEngine))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(4)
            .setPrevId(3)
            .setEngineNickName("ChitchatServingEngine")
            .setOnDemandExternalServingEngine(chitchatPreprocessEngine))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(5)
            .setPrevId(4)
            .setEngineNickName("TravelServingEngine")
            .setOnDemandExternalServingEngine(travelAgencyEngine))
      .addEngines(
        EngineInfo.newBuilder()
          .setId(6)
          .setPrevId(5)
          .setEngineNickName("ChatbotServingEngine")
          .setOnDemandPipeServingEngine(chatbotServingEngine))
      .build
  }
}
