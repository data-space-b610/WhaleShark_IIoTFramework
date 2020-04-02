package ksb.csle.examples.serving.tensorflow

import java.util.Date
import java.text.SimpleDateFormat

import scala.util.{ Try, Success, Failure }

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.BatchControlProto._
import ksb.csle.common.proto.BatchOperatorProto._
import ksb.csle.common.proto.OndemandOperatorProto.TensorflowServingOperatorInfo
import ksb.csle.common.proto.OndemandControlProto.OnDemandOperatorInfo
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.tools.client._

object SeoulTrafficServingOnK8sExample extends Logging {
  val appId = "SeoulTrafficServingOnK8sExample"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])

    logger.info("workflow: " + workflowJson)

    val client = new SimpleCsleClient("129.254.173.126", 19999)
    Try(client.submit(workflowJson,"ksbuser_etri_re_kr")) match {
      case Success(id) => logger.info("submit success:" + id)
      case Failure(e)  => logger.error("submit error", e)
    }
    client.close()
  }

  private def workflow: WorkflowInfo = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    val runType1 = RunType.newBuilder()
      .setId(1)
      .setStartTime(format.format(new Date()))
      .setPeriodic(Periodic.EVERY_MINUTE)
      .build()
    val runType2 = RunType.newBuilder()
      .setId(2)
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
          .setBatchEngine(tfEngineParam))
      .addRuntypes(runType1)
      .addEngines(
        EngineInfo.newBuilder()
          .setId(2)
          .setPrevId(1)
          .setEngineNickName("ServingEngine")
          .setOnDemandServingEngine(enginParam))
      .addRuntypes(runType2)
      .build()
  }

  private def tfEngineParam = {
    val infileInfo = FileInfo.newBuilder()
      .addFilePath("file:///home/csle/ksb-csle/examples/input/trainset.csv")
      .setFileType(FileInfo.FileType.CSV)
      .build
    val outfileInfo = FileInfo.newBuilder() // accuracy
      .addFilePath("hdfs://csle1:9000/user/ksbuser_etri_re_kr/model/rnn")
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
      .setModelPath("file:///home/csle/ksb-csle/models/rnn/model")
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
          .setPyEntryPath("hdfs://csle1:9000/user/ksbuser_etri_re_kr/dataset/recurrent/rnn_saved_model_new.py")
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

    tfEngineInfo
  }

  private def enginParam = {
    val workingDirPath = System.getProperty("user.dir")

    val port = 8003
    val modelName = "rnn"
    val modelBasePath = "hdfs://csle1:9000/user/ksbuser_etri_re_kr/model/rnn"

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
      .setClsName("ksb.csle.service.operator.TensorflowServingOperator")
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
