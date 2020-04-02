package ksb.csle.examples.tutorial

import java.util.Date
import java.text.SimpleDateFormat

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.BatchControlProto._
import ksb.csle.common.proto.BatchOperatorProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.utils.ProtoUtils

import ksb.csle.tools.client._
import ksb.csle.common.utils.resolver.PathResolver

object HourlyTensorflowTraining extends Logging {
  val appId: String = "HourlyTensorflowTraining"
  val pathPrefix = PathResolver.getDefaultFsPrefix(USERID)
  private[this] val ksbHome = sys.env("KSB_HOME")

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
    val infileInfo = FileInfo.newBuilder()
        .addFilePath("dataset/input/trainset.csv")
        .setFileType(FileInfo.FileType.CSV)
        .build
    val outfileInfo = FileInfo.newBuilder()
        .addFilePath("result")
        .setFileType(FileInfo.FileType.CSV)
        .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
    val writer = BatchWriterInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
    val dlTrainerInfo = DLTrainOperatorInfo.newBuilder()
        .setModelPath("file:///home/csle/ksb-csle/models/rnn/model/")
        .addAdditionalParams(
            ParamPair.newBuilder()
            .setParamName("isTrain")
            .setParamValue("True"))
        .addAdditionalParams(
            ParamPair.newBuilder()
            .setParamName("num_epoch")
            .setParamValue("2"))
    val operator = BatchOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.analysis.DLTrainOperator")
      .setDlTrainer(dlTrainerInfo)

      val runner = BatchRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.analysis.TensorflowRunner")
      .setTensorflowRunner(
          TensorflowRunnerInfo.newBuilder()
          .setPyEntryPath("file:///home/csle/ksb-csle/components/src/main/python/recurrent/rnn.py")
          .setCluster(false)
          .setTfVersion("r1.6"))
       .build

    val controller = BatchControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.ExternalAnalysisController")
      .setExternalAnalysisController(
          SimpleBatchControllerInfo.getDefaultInstance)

    val tfEngineInfo = BatchEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setOperator(operator)
      .setRunner(runner)
      .build

    val format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    val runType = RunType.newBuilder()
      .setId(1)
      .setStartTime(format.format(new Date()))
      .setPeriodic(Periodic.EVERY_MINUTE)
      .build()

     WorkflowInfo.newBuilder()
     .setBatch(true)
     .setMsgVersion("v1.0")
     .setKsbVersion("v1.0")
     .addEngines(
       EngineInfo.newBuilder()
       .setId(1)
       .setPrevId(0)
       .setEngineNickName("TfTrainingEngine")
       .setBatchEngine(tfEngineInfo))
      .addRuntypes(runType)
     .build
  }
}
