package ksb.csle.examples.tutorial.e2e_traffic

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.BatchControlProto._
import ksb.csle.common.proto.BatchOperatorProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

object TrafficTraining extends Logging {
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
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .setVerbose(true)
      .addRuntypes(
          RunType.newBuilder()
            .setId(1)
            .setPeriodic(Periodic.ONCE))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("TrainEngine")
            .setBatchEngine(trainParam))
      .build()
  }

  private def trainParam = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")
    val csvFilePath = s"file:///home/csle/ksb-csle/examples/output/traffic_processing.csv"
//    val csvFilePath = s"hdfs://csle1:9000/user/ksbuser_etri_re_kr/dataset/input/traffic_processing.csv"
    val infileInfo = FileInfo.newBuilder()
        .addFilePath(csvFilePath)
        .setFileType(FileInfo.FileType.CSV)
        .build
    val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)

    val runner = BatchRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.analysis.TensorflowRunner")
      .setTensorflowRunner(
          TensorflowRunnerInfo.newBuilder()
          .setPyEntryPath("file:///home/csle/ksb-csle/components/src/main/python/kangnam-customized/main_traffic_flow_cnn_04.py")
//          .setPyEntryPath("hdfs://csle1:9000/user/ksbuser_etri_re_kr/dataset/tensorflowTrainSource/kangnam-customized/main_traffic_flow_cnn_04.py")
          .setCluster(false)
          .setTfVersion("r1.6"))
      .build
      
    val controller = BatchControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.ExternalAnalysisController")
      .setExternalAnalysisController(
          SimpleBatchControllerInfo.getDefaultInstance())
          
    val dlTrainerInfo = DLTrainOperatorInfo.newBuilder()
        .setModelPath("file:///home/csle/ksb-csle/examples/output/kangnam/model")
        .addAdditionalParams(
            ParamPair.newBuilder()
            .setParamName("isTrain")
            .setParamValue("True"))
        .addAdditionalParams(
            ParamPair.newBuilder()
            .setParamName("num_epochs")
            .setParamValue("2"))
    val operator = BatchOperatorInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.operator.analysis.DLTrainOperator")
      .setDlTrainer(dlTrainerInfo)
      
    val outfileInfo = FileInfo.newBuilder()
        .addFilePath(s"hdfs://csle1:9000/user/ksbuser_etri_re_kr/model/kangnam")
        .setFileType(FileInfo.FileType.CSV)
        .build
    val writer = BatchWriterInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
      
    BatchEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setOperator(operator)
      .setRunner(runner)
      .build()
  }

}
