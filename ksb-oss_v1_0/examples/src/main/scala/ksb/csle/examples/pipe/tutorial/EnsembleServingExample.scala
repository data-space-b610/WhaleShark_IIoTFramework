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
object EnsembleServingExample extends Logging {
  private val appId = this.getClass.getSimpleName.replace("$","")
  private val host = "0.0.0.0"
  private val ksbHome = sys.env("KSB_HOME")

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

  val firstEnsemble_preprocess = s"http://${host}:18001"
  val secondEnsemble_preprocess = s"http://${host}:18002"
//  val firstEnsemble_model = "http://localhost:8001"
//  val secondEnsemble_model = "http://localhost:8001"
//
//  private def firstEnsembblePreprocessEngine = {
//    val runner = OnDemandExternalRunnerInfo.newBuilder()
//      .setClsName("dummyClass")
//      .setPyContainerRunner(
//          PyContainerRunnerInfo.newBuilder()
//            .setPort(18001)
//            .setImgName("ssallys/firstEnsemble_image")
//            .setCodePath(s"file:///${ksbHome}/dockerImages/Serving/firstEnsembble")
//            .setLibraries("nltk:numpy")
//            .setUserArgs(s"http://${host}:8001/model"))
//      .build()
//    OnDemandExternalEngineInfo.newBuilder()
//      .setRunner(runner)
//      .build
//  }
//
//  private def secondEnsembblePreprocessEngine = {
//    val runner = OnDemandExternalRunnerInfo.newBuilder()
//      .setClsName("dummyClass")
//      .setPyContainerRunner(
//          PyContainerRunnerInfo.newBuilder()
//            .setPort(18002)
//            .setImgName("ssallys/secondEnsemble_image")
//            .setCodePath(s"file:///${ksbHome}/dockerImages/Serving/secondEnsembble")
//            .setLibraries("nltk:numpy")
//            .setUserArgs(s"http://${host}:8002/model"))
//      .build()
//    OnDemandExternalEngineInfo.newBuilder()
//      .setRunner(runner)
//      .build
//  }

  private def workflow = {
    val outCumulateClassName = "ksb.csle.component.pipe.query.operator.OutputCumulativePipeOperator"
    val outAggregateClassName = "ksb.csle.component.pipe.query.operator.OutputAggregatePipeOperator"

    // Query to model server.
    val operator1 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(outCumulateClassName)
      .setOutputCumulativeOperator(
          RestfulQueryPipeOperatorInfo.newBuilder()
          .setUrl(firstEnsemble_preprocess)
          .setMethod(RestfulQueryPipeOperatorInfo.Method.POST)
          .addHeader(
              ParamPair.newBuilder()
              .setParamName("Content-Type")
              .setParamValue("text/plain; charset=utf-8")
              .build()))

    val operator2 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName(outCumulateClassName)
      .setOutputCumulativeOperator(
          RestfulQueryPipeOperatorInfo.newBuilder()
          .setUrl(secondEnsemble_preprocess)
          .setMethod(RestfulQueryPipeOperatorInfo.Method.POST)
          .addHeader(
              ParamPair.newBuilder()
              .setParamName("Content-Type")
              .setParamValue("text/plain; charset=utf-8")
              .build()))

    val operator3 = OnDemandPipeOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName(outAggregateClassName)
      .setOutputAggregateOperator(
          OutputAggregatePipeOperatorInfo.newBuilder()
          .setMethod(OutputAggregatePipeOperatorInfo.Method.AVG)
          .setSeparator(" ")
          .setArrayDepth(2))

    val runner = OnDemandRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.runner.ServingPipeRunner")
      .setWebRunner(
          WebserviceRunnerInfo.newBuilder()
          .setHost("0.0.0.0")
          .setPort(18080))

    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.OnDemandCompositeServingRestfulController")
      .setOnDemandCompositeServingController(
          SimpleOnDemandControllerInfo.newBuilder()
          .setRestfulActorName("ksb.csle.component.pipe.controller.actor.EnsembleServingActor")
          .setUri("postQuery"))

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
      .addEngines(
        EngineInfo.newBuilder()
          .setId(1)
          .setPrevId(0)
          .setEngineNickName("EnsembleServingEngine")
          .setOnDemandPipeServingEngine(multipleQueryEngineInfo))
      .build
  }
}
