package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.OndemandOperatorProto.TensorflowServingOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.OnDemandOperatorInfo
import ksb.csle.common.utils.ProtoUtils

import ksb.csle.tools.client._
import ksb.csle.common.utils.resolver.PathResolver

object ClassifierServingOnContainerExample extends Logging {

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

  private def workflow: WorkflowInfo = {
    val runType1 = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()
    val runType2 = RunType.newBuilder()
      .setId(2)
      .setPeriodic(Periodic.ONCE)
      .build()

    WorkflowInfo.newBuilder()
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType1)
      .addRuntypes(runType2)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("ClassificationServingEngine")
            .setOnDemandExternalServingEngine(classificationPreprocessEngine))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(2)
            .setPrevId(1)
            .setEngineNickName("ChitchatServingEngine")
            .setOnDemandExternalServingEngine(chitchatPreprocessEngine))
      .build()
  }

  private def classificationPreprocessEngine = {
    val runner = OnDemandExternalRunnerInfo.newBuilder()
      .setClsName("dummyClass")
      .setPyContainerRunner(
          PyContainerRunnerInfo.newBuilder()
            .setPort(18001)
            .setImgName("ssallys/classify_image")
            .setCodePath(s"file:///${ksbHome}/dockerImages/Serving/classify")
            .setLibraries("nltk:numpy")
            .setUserArgs("http://localhost:8001/model"))
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
            .setImgName("ssallys/chitchat_image")
            .setCodePath(s"file:///${ksbHome}/dockerImages/Serving/chitchat")
            .setLibraries("nltk:numpy")
            .setUserArgs("http://localhost:8002/model"))
      .build()

    val nginInfo = OnDemandExternalEngineInfo.newBuilder()
      .setRunner(runner)
      .build

    nginInfo
  }
}
