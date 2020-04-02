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

object TfStreamPredictionTraffic extends Logging {
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
    val httpServerPort = 53551

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(1)
            .setWindowSize(1440 * 24)
            .setSlidingSize(1440))

    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.HttpServerReader")
      .setHttpServerReader(
        HttpServerInfo.newBuilder()
          .setPort(httpServerPort))

    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "group2"
    val kafkaTopic = "traffic_output"
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
      .setSparkRunner(SparkRunnerInfo.newBuilder()
          .setSparkArgs(SparkArgs.newBuilder()
              .setDriverMemory("1g")
              .setExecuterMemory("1g")))

    val operator1 = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(SelectColumnsInfo.newBuilder()
          .addAllSelectedColumnName(
              Seq("YEAR", "MON", "DAY", "HOUR", "MIN", "LINK_ID", "SPD").asJava))
      .build()

    val operator2 = StreamOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.operator.integration.ConcatAndReshapeOperator")
      .setReshapeWithConcat(ReshapeWithConcatInfo.newBuilder()
          .addSelectedColumnId(0) // YEAR
          .addSelectedColumnId(1) // MON
          .addSelectedColumnId(2) // DAY
          .addSelectedColumnId(3) // HOUR
          .addSelectedColumnId(4) // MIN
          .setDelimiter("_")
          .setValueColName("DATE_TIME")
          .setCondition(ReshapeWithConcatInfo.Condition.KEEP_ORIGINAL_AND_RESULT))
      .build()

    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(MinMaxScalingInfo.newBuilder()
          .addSelectedColumnId(6) // SPD
          .setMax("0.5")
          .setMin("-0.5")
          .setWithMinMaxRange(true)
          .setMaxRealValue("100")
          .setMinRealValue("0"))
      .build()

    val operator4 = StreamOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.operator.transformation.PivotOperator")
      .setPivot(PivotInfo.newBuilder()
          .setSelectedColumnId(5) // LINK_ID
          .setGroupByColumn("7") //DATE_TIME
          .setValueColumn("6") // SPD
          .setMethod(PivotInfo.Method.AVG))
      .build()

    val columnIds = Array.range(1, 1383).map(new java.lang.Integer(_)).toSeq
    val operator5 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(SelectColumnsInfo.newBuilder()
          .addAllSelectedColumnId(columnIds))
      .build()

    val operator6 = StreamOperatorInfo.newBuilder()
      .setPrevId(5)
      .setId(6)
      .setClsName("ksb.csle.component.operator.cleaning.MissingValueImputeOperator")
      .setMissingValueImputation(MissingValueImputationInfo.newBuilder()
          .setScope(MissingValueImputationInfo.Scope.SCOPE_ALL)
          .addSelectedColumnId(-1) // all columns
          .setMethod(MissingValueImputationInfo.Method.SPECIFIC_VALUE)
          .setHow(MissingValueImputationInfo.How.HOW_ANY)
          .addSubParam(SubParameter.newBuilder
              .setKey("numeric")
              .setValue("0")))
      .build()

    val operator7 = StreamOperatorInfo.newBuilder()
      .setPrevId(6)
      .setId(7)
      .setClsName("ksb.csle.component.operator.integration.VectorAssembleColumnAddOperator")
      .setAddVectorAssembleColumn(AddVectorAssembleColumnInfo.newBuilder()
          .setVectorAssembleColumnName("in"))
      .build()

    val operator8 = StreamOperatorInfo.newBuilder()
      .setPrevId(7)
      .setId(8)
      .setClsName("ksb.csle.component.operator.transformation.FlattenOperator")
      .setFlatten(FlattenInfo.newBuilder()
          .setColumnName("in"))

    val workingDirPath = System.getProperty("user.dir")
    val modelBasePath = s"$workingDirPath/../examples/models/rnn/model"
    val operator10 = StreamOperatorInfo.newBuilder()
      .setPrevId(9)
      .setId(10)
      .setClsName("ksb.csle.component.operator.analysis.TensorflowPredictOperator")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri(modelBasePath)
          .setModelName("seoul_traffic")
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
      .addOperator(operator8)
      .addOperator(operator10)
      .setWriter(writer)
      .setRunner(runner)
      .build()
  }
}
