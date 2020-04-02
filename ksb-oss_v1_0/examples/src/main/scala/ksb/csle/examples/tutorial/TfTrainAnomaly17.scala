package ksb.csle.examples.tutorial

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
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._
import ksb.csle.common.utils.resolver.PathResolver

object TfTrainAnomaly17 extends Logging {
  val appId = "TfTrainAnomaly17"

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
    val datasetDir = s"${ksbHome}/examples/datasets/anomaly17"

    val infileInfo = FileInfo.newBuilder()
        .addFilePath("NOT_SET")
        .build
    val outfileInfo = FileInfo.newBuilder()
        .addFilePath("NOT_SET")
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
              .setParamValue("/tmp/anomaly17"))
        .addAdditionalParams(
            ParamPair.newBuilder()
              .setParamName("MODEL_VERSION")
              .setParamValue("1"))
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
          .setPyEntryPath(s"file:///${ksbHome}/components/src/main/python/anomaly17/tf_main_csle.py")
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
       .addRuntypes(
           RunType.newBuilder()
             .setId(1)
             .setPeriodic(Periodic.ONCE))
     .build
  }
}
