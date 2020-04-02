package ksb.csle.examples.tutorial

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

object TfStreamPredictionKangnamTraffic extends Logging {
  val appId = "TfStreamPredictionTraffic"

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
    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()
    WorkflowInfo.newBuilder()
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("StreamToStreamEngine")
            .setStreamToStreamEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")
    val httpServerPort = 53552

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(5)
            .setWindowSize(170 * 8)
            .setSlidingSize(170))

    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.HttpServerReader")
      .setHttpServerReader(
        HttpServerInfo.newBuilder()
          .setPort(httpServerPort))

//    val writer = StreamWriterInfo.newBuilder()
//      .setId(1)
//      .setPrevId(0)
//      .setClsName(stdoutWriterClassName)
//      .setStdoutWriter(StdoutWriterInfo.getDefaultInstance())
    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "kangnam_output"
    val kafkaTopic = "kangnam_output"
    val writer = StreamWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(KafkaInfo.newBuilder()
          .setBootStrapServers(kafkaServer)
          .setZooKeeperConnect(kafkaZookeeper)
          .setGroupId(kafkaGroupId)
          .setTopic(kafkaTopic))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
          .setSparkArgs(
            SparkArgs.newBuilder()))

    val operator1 = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(SelectColumnsInfo.newBuilder()
          .addAllSelectedColumnName(
              Seq("PRCS_DATE","LINK_ID","PRCS_SPD").asJava))
      .build()

    val operator2 = StreamOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(MinMaxScalingInfo.newBuilder()
          .addSelectedColumnId(2) // PRCS_SPD
          .setMax("0.5")
          .setMin("-0.5")
          .setWithMinMaxRange(true)
          .setMaxRealValue("100")
          .setMinRealValue("0"))
      .build()

    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.operator.transformation.PivotOperator")
      .setPivot(PivotInfo.newBuilder()
          .setSelectedColumnId(1) // LINK_ID
          .setGroupByColumn("0") // PRCS_DATE
          .setValueColumn("2") // PRCS_SPD
          .setMethod(PivotInfo.Method.AVG))
      .build()

    val operator4 = StreamOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(SelectColumnsInfo.newBuilder()
          .addAllSelectedColumnName(
              Seq("1220019200","1220003200","1220027800","1210013700","1210030600","1220025900","1220025100","1220028900","1220036800","1220029200","1220026200","1210030100","1220024800","1220007700","1220016200","1220031000","1220029700","1220035200","1210006300","1220023300","1220032800","1220036400","1220036900","1220016300","1220021800","1220009000","1220033500","1220032300","1220030000","1220027400","1220034100","1220030700","1220033300","1220002200","1220031800","1220027700","1220023100","1220026600","1220027200","1220028000","1220037000","1220019900","1220024900","1220024700","1220022600","1220028300","1220002800","1220035500","1220033100","1220021000","1220027600","1220030800","1220019000","1220028800","1220034900","1220036600","1220013400","1220025700","1220007600","1220036700","1220034500","1220034400","1220029900","1220013500","1220032100","1220030900","1220004800","1220029800","1220029500","1210006200","1220021100","1220001600","1220023400","1220034700","1220037300","1220022900","1220003100","1220033400","1220026900","1220033200","1220031100","1220016100","1220016000","1220020800","1210013600","1220032900","1220030400","1220032600","1220030600","1220001900","1220002700","1220025000","1220015500","1220029400","1220026000","1220011800","1220009100","1220020900","1220036500","1220026100","1220023000","1220021400","1220031300","1220019100","1220002300","1220024600","1220004900","1220029600","1220025200","1220033600","1220033700","1220003400","1220031900","1220019800","1210030700","1220029100","1210030000","1220035400","1220024200","1210011300","1220005300","1220032000","1220034600","1220037200","1220029300","1220026300","1220027900","1220026800","1220030500","1220024000","1220004000","1220028100","1220005200","1220004100","1220004200","1220011900","1220034200","1220004300","1220021500","1220002000","1220027500","1220030100","1220032700","1220025300","1220035300","1220026700","1220016600","1220027300","1220001500","1220016700","1220031200","1220019300","1220021900","1220022500","1220033000","1220034300","1220032200","1220024300","1220003300","1220025600","1220028200","1220034800","1220015400","1220029000","1220023200","1220034000","1220024100","1210011200","1220025800","1220037100").asJava))
      .build()

    val operator5 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.component.operator.integration.VectorAssembleColumnAddOperator")
      .setAddVectorAssembleColumn(AddVectorAssembleColumnInfo.newBuilder()
          .setVectorAssembleColumnName("in1"))
      .build()

    val operator6 = StreamOperatorInfo.newBuilder()
      .setPrevId(5)
      .setId(6)
      .setClsName("ksb.csle.component.operator.transformation.FlattenOperator")
      .setFlatten(FlattenInfo.newBuilder()
          .setColumnName("in1"))

    val workingDirPath = System.getProperty("user.dir")
    val modelBasePath = s"$workingDirPath/../examples/models/kangnam/model"
    val operator7 = StreamOperatorInfo.newBuilder()
      .setPrevId(6)
      .setId(7)
      .setClsName("ksb.csle.component.operator.analysis.TensorflowPredictOperator")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri(modelBasePath)
          .setModelName("kangnam_traffic")
          .setSignatureName("predict_speed"))
      .build()

    StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .addOperator(operator7)
      .setWriter(writer)
      .setRunner(runner)
      .build()
  }
}
