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
import ksb.csle.common.utils.resolver.PathResolver

import ksb.csle.tools.client._

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object BatchTfTrainInSingleEngine extends Logging {
  val appId: String = "Data-BatchTfTrainInSingleEngine"

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
      .addFilePath("file:///home/csle/ksb-csle/examples/input/trainset.csv")
      .setFileType(FileInfo.FileType.CSV)
      .build
    val outfileInfo = FileInfo.newBuilder() // accuracy
      .addFilePath(s"hdfs://csle1:9000/user/ksbuser_etri_re_kr/model/rnn")
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
            .setParamName("model_filename")
            .setParamValue("model.ckpt"))
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
            .setPyEntryPath("file:///home/csle/ksb-csle/components/src/main/python/recurrent/rnn_saved_model_new.py")
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

    // TODO: Use ExternalEngine when our system adopt CODDL.
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
