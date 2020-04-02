package ksb.csle.examples.tutorial.k8s

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.OndemandOperatorProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.tools.client._
import ksb.csle.examples.tutorial._

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object RealtimeKafkaIngestToServingWithKbInTwoEngines extends Logging {

  val appId: String = "RealtimeKafkaIngestToServingWithKbInTwoEngines"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])
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
     WorkflowInfo.newBuilder()
     .setBatch(false)
     .setMsgVersion("v1.0")
     .setKsbVersion("v1.0")
     .addEngines(
       EngineInfo.newBuilder()
       .setId(1)
       .setPrevId(0)
       .setEngineNickName("IngestionEngine")
       .setStreamToStreamEngine(getKafkaIngestToPredictParamInOne))
     .addEngines(
       EngineInfo.newBuilder()
       .setId(2)
       .setPrevId(1)
       .setEngineNickName("ServingEngine")
       .setOnDemandStreamServingEngine(getServingWithKbParam))
      .addRuntypes(
        RunType.newBuilder()
       .setId(1)
       .setPeriodic(Periodic.ONCE))
      .addRuntypes(
        RunType.newBuilder()
       .setId(2)
       .setPeriodic(Periodic.ONCE))
     .build()
  }
}
