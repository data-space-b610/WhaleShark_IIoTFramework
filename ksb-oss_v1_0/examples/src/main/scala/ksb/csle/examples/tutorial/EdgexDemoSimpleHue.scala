package ksb.csle.examples.tutorial

import scala.util.{Try, Success, Failure}

import org.apache.logging.log4j.scala.Logging

import com.google.protobuf.Message

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.DatasourceProto.HttpClientInfo._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.FilterInfo._
import ksb.csle.common.proto.StreamOperatorProto.AddConditionalColumnInfo._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils

import ksb.csle.tools.client._

object EdgexDemoSimpleHue extends Logging {
  val appId = "EdgexDemoSimpleHue"

  def main(args: Array[String]) {
    val workflowJson = ProtoUtils.msgToJson(workflow.asInstanceOf[Message])
    logger.info(workflowJson)

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
            .setEngineNickName(appId)
            .setStreamToStreamEngine(enginParam))
      .build()
  }

  private def enginParam = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")
    val httpServerPort = 57071

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkSessionOrStreamController(
        SimpleBatchOrStreamControllerInfo.newBuilder()
          .setOperationPeriod(1))

    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.HttpServerReader")
      .setHttpServerReader(
        HttpServerInfo.newBuilder()
          .setPort(httpServerPort))

    val writer = StreamWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.HttpClientWriter")
      .setHttpClientWriter(HttpClientInfo.newBuilder()
          .setUrl("http://10.100.0.121:48082/api/v1/device/5b8756529f8fc2000183fe08/command/5b8752139f8fc2000183fded")
          .setMethod(Method.PUT)
          .addHeader(HttpParam.newBuilder()
              .setKey("Content-Type")
              .setValue("application/json"))
          .setSrcColumnName("hueCommand"))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(SparkRunnerInfo.newBuilder()
          .setSparkArgs(SparkArgs.newBuilder()
              .setDriverMemory("1g")
              .setExecuterMemory("1g")
              .setExecutorCores("4")))

    val operator1 = StreamOperatorInfo.newBuilder()
      .setPrevId(0)
      .setId(1)
      .setClsName("ksb.csle.component.operator.transformation.ExplodeColumnOperator")
      .setExplodeColumn(
          ExplodeColumnInfo.newBuilder().setSrcColumnName("readings"))

    val operator2 = StreamOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.operator.transformation.SplitColumnOperator")
      .setSplitColumn(
          SplitColumnInfo.newBuilder().setSrcColumnName("readings"))

    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.operator.reduction.FilterOperator")
      .setFilter(FilterInfo.newBuilder()
          .setColName("name")
          .setCondition(Condition.EXIST)
          .setPattern("temperature"))

    val operator4 = StreamOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.operator.transformation.ChangeColumnDataTypeOperator")
      .setChangeColumnDataType(ChangeColumnDataTypeInfo.newBuilder()
          .setColumName("value")
          .setDataType(FieldType.DOUBLE))

    val operator5 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.component.operator.transformation.AddConditionalColumnOperator")
      .setAddConditionalColumn(AddConditionalColumnInfo.newBuilder()
          .setNewColumnName("hueCommand")
          .addRules(AddColumnRule.newBuilder()
              .setCondition("name = 'temperature' and value >= 30")
              .setValue("""{"id":"2","state":{"on":true,"hue":65535}}"""))
          .addRules(AddColumnRule.newBuilder()
              .setCondition("name = 'temperature' and (value > 15 and value < 30)")
              .setValue("""{"id":"2","state":{"on":true,"hue":21845}}"""))
          .addRules(AddColumnRule.newBuilder()
              .setCondition("name = 'temperature' and value <= 15")
              .setValue("""{"id":"2","state":{"on":true,"hue":43690}}""")))

    StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setRunner(runner)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .setWriter(writer)
      .build()
  }
}
