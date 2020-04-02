//package ksb.examples.custom
//
//import scala.util.{Try, Success, Failure}
//
//import org.apache.logging.log4j.scala.Logging
//import com.google.protobuf.Message
//
//import ksb.csle.common.proto.CustomDatasourceProto._
//import ksb.csle.common.proto.StreamOperatorProto._
//import ksb.csle.common.proto.DatasourceProto._
//import ksb.csle.common.proto.RunnerProto._
//import ksb.csle.common.proto.WorkflowProto._
//import ksb.csle.common.proto.CustomOperatorProto._
//import ksb.csle.common.proto.StreamControlProto._
//import ksb.csle.common.proto.OndemandControlProto._
//import ksb.csle.common.proto.SharedProto._
//import ksb.csle.common.utils.ProtoUtils
//import ksb.csle.common.utils.config.ConfigUtils
//
//import ksb.csle.tools.client._
//
//object MyFileReaderExample extends Logging {
//  val appId: String = "Data-TrafficExample"
//
//  def main(args: Array[String]) {
//    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])
//    val client = new SimpleCsleClient("localhost", 19999)
//    Try (client.submit(
//        workflowJson,
//        "ksbuser@etri.re.kr",
//        this.getClass.getSimpleName.replace("$",""),
//        this.getClass.getSimpleName.replace("$",""))) match {
//      case Success(id) => logger.info("submit success:" + id)
//      case Failure(e) => logger.error("submit error", e)
//    }
//    client.close()
//  }
//
//  // ClientSDK를 이용한 워크플로우 사양 작성
//  private def workflow = {
//
//    // 새로 개발한 MyFileReader를 이용하기 위하여 proto 메세지 인스턴스를 생성하고 이를 MyFileReader를 클래스에 바인딩하기 위한 사양을 정의한다.
//    val infileInfo = MyFileInfo.newBuilder()
//      .addFilePath(
//        s"file:///home/csle/ksb-csle/examples/input/adult.csv"
//        .replaceAll("\\\\", "/"))
//      .setFileType(MyFileInfo.FileType.CSV)
//      .setDelimiter(";")
//      .setHeader(true)
//      .build
//    val reader = BatchReaderInfo.newBuilder()
//      .setId(1)
//      .setPrevId(0)
//      .setClsName("ksb.csle.component.reader.custom.MyFileReader")
//      .setMyFileReader(infileInfo)
//
//    val selectedColumnsWithFileInfo =
//      SelectColumnsWithFileInfo.newBuilder()
//      .setColumnIdPath(
//          s"file:///home/csle/ksb-csle/examples/input/columnSelection_adult.csv")
//      .build
//    val operator = StreamOperatorInfo.newBuilder()
//      .setId(1)
//      .setPrevId(0)
//      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectWithFileOperator")
//      .setSelectColumnsWithFile(selectedColumnsWithFileInfo)
//      .build
//
//    val outfileInfo = FileInfo.newBuilder()
//      .addFilePath(
//          s"output/output_columnSelect_adult.csv"
//          .replaceAll("\\\\", "/"))
//      .setFileType(FileInfo.FileType.CSV)
//      .setDelimiter(",")
//      .setHeader(true)
//      .build
//    val writer = BatchWriterInfo.newBuilder()
//      .setId(11)
//      .setPrevId(10)
//      .setClsName("ksb.csle.component.writer.FileWriter")
//      .setFileWriter(outfileInfo)
//
//    val runner = StreamRunnerInfo.newBuilder()
//      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
//      .setSparkRunner(
//          SparkRunnerInfo.getDefaultInstance)
//
//    val controller = StreamControllerInfo.newBuilder()
//      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
//      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)
//
//    // 각 컴퍼넌트 사양을 조합하여 하나의 엔진 사양을 구성
//    val dataEngineInfo = BatchToBatchStreamEngineInfo.newBuilder()
//      .setController(controller)
//      .setReader(reader)
//      .setWriter(writer)
//      .addOperator(operator)
//      .setRunner(runner)
//      .build
//
//    val runType = RunType.newBuilder()
//      .setId(1)
//      .setPeriodic(Periodic.ONCE)
//      .build()
//
//    // 작성한 엔진사양들을 이용하여 하나의 워크플로우 흐름 조합
//    WorkflowInfo.newBuilder()
//      .setBatch(true)
//      .setMsgVersion("v1.0")
//      .setKsbVersion("v1.0")
//      .setVerbose(true)
//      .addRuntypes(runType)
//      .addEngines(
//          EngineInfo.newBuilder()
//            .setId(1)
//            .setPrevId(0)
//            .setEngineNickName("DataEngine")
//            .setBatchToBatchStreamEngine(dataEngineInfo))
//      .build
//  }
//}
