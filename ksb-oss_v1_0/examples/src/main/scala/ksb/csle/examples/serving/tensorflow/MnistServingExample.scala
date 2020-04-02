package ksb.csle.examples.serving.tensorflow

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

object MnistServingExample extends Logging {
  val appId = "MnistServingExample"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("localhost", 19999)
    Try (client.submit(workflowJson)) match {
      case Success(id) => logger.info("submit success:" + id)
      case Failure(e) => logger.error("submit error", e)
    }
    client.close()
  }

  private def workflow: WorkflowInfo = {

    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("ServingEngine")
            .setOnDemandServingEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val workingDirPath = System.getProperty("user.dir")

    val port = 8001
    val modelName = "mnist"
    val modelBasePath = s"$workingDirPath/models/mnist/model"

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
}
