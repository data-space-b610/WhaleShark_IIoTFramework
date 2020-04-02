package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message

import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.BatchControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.OndemandOperatorProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.AutoSparkMlProto._
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._
import ksb.csle.common.utils.resolver.PathResolver

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object BatchMLTrainInSingleEngine extends Logging {
  val appId: String = "BatchMLTrainInSingleEngine"
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

  private def workflow: WorkflowInfo = {
    val userHome = ConfigUtils.getConfig().envOrElseConfig("csle.user.home")
    val infileInfo = FileInfo.newBuilder()
      .addFilePath(s"dataset/BatchAutoMLTrainInSingleEngine/hue_train_dataset"
          .replaceAll("\\\\", "/"))
          .setFileType(FileInfo.FileType.PARQUET)
      .build
      val outfileInfo = FileInfo.newBuilder()
      .addFilePath("autosparkml")
      .setFileType(FileInfo.FileType.CSV)
      .build
      val reader = BatchReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.FileReader")
      .setFileReader(infileInfo)
      val writer = BatchWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.writer.FileWriter")
      .setFileWriter(outfileInfo)
      val runner = BatchRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.PySparkRunner")
      .setPySparkMLRunner(
          PySparkMLRunnerInfo.newBuilder()
          .setPyEntryPath("file:////home/csle/ksb-csle/pyML/autosparkml/bridge/call_trainer.py")
          .setInJson(false)
          .setSparkArgs(
            SparkArgs.newBuilder()
            .setMemory("4g")
            .setExecutorCores("2"))
        )
       .build
      val MLInfo = DecisionTreeClassifier.newBuilder()
        .addMaxDepth(1)
        .addMaxBins(1)
        .build()
      val operator = BatchOperatorInfo.newBuilder
        .setId(2)
        .setPrevId(1)
        .setClsName("DummyClass")
        .setDecisionTreeClassifier(MLInfo)
      val controller = BatchControllerInfo.newBuilder()
        .setClsName("ksb.csle.component.controller.PySparkMLTrainer")
        .setPyMLTrainer(
            SimpleBatchControllerInfo.newBuilder())
      val analysisMLInfo = ExternalEngineInfo.newBuilder()
        .setController(controller)
        .setReader(reader)
        .setWriter(writer)
        .setRunner(runner)
        .addOperator(operator)
        .build

      WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addEngines(
          EngineInfo.newBuilder()
          .setId(1)
          .setPrevId(0)
          .setEngineNickName("MLEngine")
          .setExternalEngine(analysisMLInfo))
      .addRuntypes(
        RunType.newBuilder()
       .setId(1)
       .setPeriodic(Periodic.ONCE))
      .build()
  }
}
