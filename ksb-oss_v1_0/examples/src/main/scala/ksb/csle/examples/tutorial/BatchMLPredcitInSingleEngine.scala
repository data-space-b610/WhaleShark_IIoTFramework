package ksb.csle.examples.tutorial

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
import ksb.csle.common.proto.AutoSparkMlProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.utils.ProtoUtils

import ksb.csle.tools.client._

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */

object BatchMLPredcitInSingleEngine extends Logging {

  val appId: String = "MLPredcitWorkflow"

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

  def workflow: WorkflowInfo = {
    val infileInfo = FileInfo.newBuilder()
        .addFilePath(
            "dataset/BatchAutoMLTrainInSingleEngine/hue_train_dataset/part-00000-3e83442f-ea26-48de-b337-94a096e7687a.snappy.parquet")
        .setFileType(FileInfo.FileType.PARQUET)
        .build
      val outfileInfo = FileInfo.newBuilder()
      .addFilePath("autosparkml")
      .setFileType(FileInfo.FileType.JSON)
      .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)

    val operator = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.analysis.SparkMLPredictOperator")
      .setMlStreamPredictor(
          SparkMLStreamPredictOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setModelPath("file:///home/csle/ksb-csle/examples/autosparkml/test/automl_test/0000")
          .build)

    val writer = BatchWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
          .setSparkArgs(
            SparkArgs.newBuilder()
            .setMemory("4g")
            .setExecutorCores("8")))
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val predictInfo = BatchToBatchStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .addOperator(operator)
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
       .setEngineNickName("PredictEngine")
       .setBatchToBatchStreamEngine(predictInfo))
      .addRuntypes(
        RunType.newBuilder()
       .setId(1)
       .setPeriodic(Periodic.ONCE))
     .build()
  }
}
