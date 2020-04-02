package ksb.csle.examples.tutorial

import java.util.Date
import java.text.SimpleDateFormat

import scala.util.{Try, Success, Failure}

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
import ksb.csle.common.utils.resolver.PathResolver

object Anomaly17TrainAndServing extends Logging {
  val appId = "Anomaly17TrainAndServing"

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
    val exportPath = "file:///tmp/anomaly17"

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("training_anomaly17")
            .setBatchEngine(trainingEngine(exportPath)))
      .addRuntypes(
          RunType.newBuilder()
            .setId(1)
            .setPeriodic(Periodic.ONCE))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(2)
            .setPrevId(1)
            .setEngineNickName("serving_anomaly17")
            .setOnDemandServingEngine(servingEngine(exportPath)))
      .addRuntypes(
          RunType.newBuilder()
            .setId(2)
            .setPeriodic(Periodic.ONCE))
    .build
  }

  private def trainingEngine(exportPath: String) = {
    val datasetDir = "/home/csle/ksb-csle/examples/datasets/anomaly17"
//    val modelVersion = (System.currentTimeMillis() / 1000).toString()
    val modelVersion = "1"

    val infileInfo = FileInfo.newBuilder()
        .addFilePath("no_input")
        .build
    val outfileInfo = FileInfo.newBuilder()
        .addFilePath("no_output")
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

    // ('--USE_DEMO_DATA',        type='bool', default=True,                  help='use demo data (wave combinations)')
    // ('--TRAIN_DATA_X_PATH',    type=str, default='./data/train_x.csv',     help='train data x file path')
    // ('--TRAIN_DATA_Y_PATH',    type=str, default='./data/train_y.csv',     help='train data y file path')
    // ('--PREDICT_DATA_X_PATH',  type=str, default='./data/test_x.csv',      help='predict data x file path')
    // ('--VALIDATE_DATA_X_PATH', type=str, default='./data/test_x.csv',      help='validation data x file path')
    // ('--VALIDATE_DATA_Y_PATH', type=str, default='./data/test_y.csv',      help='validation data y file path')
    // ('--CA_DATA_X_PATH',       type=str, default='./data/test_x.csv',      help='ca data x file path')
    // ('--CA_DATA_Y_PATH',       type=str, default='./data/test_y.csv',      help='ca data y file path')
    // ("--EXPORT_URL",           type=str, default="/tmp/anomaly2017",       help="url to export a saved model")
    // ("--MODEL_VERSION",        type=str, default="1",                      help="model version")
    // ('--LOG_DIR',              type=str, default='./tf_logs/',             help='log directory path')
    // ("--SAVE_PATH",            type=str, default="/tmp/anomaly_2017.ckpt", help="save path")
    val dlTrainerInfo = DLTrainOperatorInfo.newBuilder()
        .setModelPath("")
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("USE_DEMO_DATA")
              .setParamValue("False"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("TRAIN_DATA_X_PATH")
              .setParamValue(s"$datasetDir/train_x.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("TRAIN_DATA_Y_PATH")
              .setParamValue(s"$datasetDir/train_y.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("PREDICT_DATA_X_PATH")
              .setParamValue(s"$datasetDir/test_x.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("VALIDATE_DATA_X_PATH")
              .setParamValue(s"$datasetDir/test_x.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("VALIDATE_DATA_Y_PATH")
              .setParamValue(s"$datasetDir/test_y.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("CA_DATA_X_PATH")
              .setParamValue(s"$datasetDir/test_x.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("CA_DATA_Y_PATH")
              .setParamValue(s"$datasetDir/test_y.csv"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("EXPORT_URL")
              .setParamValue(exportPath))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("MODEL_VERSION")
              .setParamValue(modelVersion))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("LOG_DIR")
              .setParamValue("/tmp/anomaly17_tf_logs"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("SAVE_PATH")
              .setParamValue("/tmp/anomaly17.ckpt"))
    val operator = BatchOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.analysis.DLTrainOperator")
      .setDlTrainer(dlTrainerInfo)

    val runner = BatchRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.analysis.TensorflowRunner")
      .setTensorflowRunner(
          TensorflowRunnerInfo.newBuilder()
          .setPyEntryPath(s"file:///home/csle/ksb-csle/components/src/main/python/anomaly17/tf_main_csle.py")
          .setCluster(false)
          .setTfVersion("r1.6"))
      .build

    val controller = BatchControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.ExternalAnalysisController")
      .setExternalAnalysisController(
          SimpleBatchControllerInfo.getDefaultInstance)

    BatchEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setOperator(operator)
      .setRunner(runner)
      .build
  }

  private def servingEngine(modelBasePath: String) = {
    val port = 7077
    val modelName = "anomaly17"
//    val modelBasePath = s"file://${ksbHome}/examples/models/anomaly17/model"

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

    OnDemandServingEngineInfo.newBuilder()
      .setController(controller)
      .setRunner(runner)
      .setOperator(predictOper)
      .build
  }
}
