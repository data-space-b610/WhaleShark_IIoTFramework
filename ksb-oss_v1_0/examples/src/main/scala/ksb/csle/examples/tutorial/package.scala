package ksb.csle.examples

import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging
import org.apache.log4j.Level
import org.apache.log4j.Logger

import com.google.protobuf.Message

import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.OndemandOperatorProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.OndemandControlProto._
import ksb.csle.common.proto.OndemandOperatorProto.SparkMLStreamPredictOperatorInfo
import ksb.csle.common.proto.KnowledgeProto._
import ksb.csle.common.proto.SharedProto._
import ksb.csle.common.base.workflow.BaseWorkflow
import ksb.csle.common.base.controller.BaseOrchestrator
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.resolver.PathResolver

package object tutorial {

  val USERID = "leeyh@etri.re.kr"
  val pathPrefix = PathResolver.getDefaultFsPrefix(USERID)
  val userHome = ConfigUtils.getConfig().envOrElseConfig("csle.user.home")
  val ksbHome = sys.env("KSB_HOME")
  val hadoopMaster = ConfigUtils.getConfig().envOrElseConfig("servers.hadoop.master")

  private[this] val bootStrapServers: String = "csle1:9092"
  private[this] val zooKeeperConnect: String = "csle1:2181"
  private[this] val modelPath: String = "file:///home/csle/ksb-csle/examples/autosparkml/test/automl_test"

  private[this] val sparkCluster =
      SparkArgs.newBuilder()
      .setExtraArgs( userHome + "/hadoop/etc/hadoop/")
      .setNumExecutors("3")
      .setDriverMemory("4g")
      .setExecuterMemory("4g")
      .setExecutorCores("2")
      .build

  val getIngestionParam:StreamToStreamEngineInfo = getIngestionParam(bootStrapServers, zooKeeperConnect)
  val getDataParam: StreamToStreamEngineInfo = getDataParam(bootStrapServers, zooKeeperConnect)
  val getPredictParam: StreamToStreamEngineInfo = getPredictParam(bootStrapServers, zooKeeperConnect)
  val getServingParam: OnDemandStreamServingEngineInfo = getServingParam(bootStrapServers, zooKeeperConnect)
  val getIngestToPredictParamInOne: StreamToStreamEngineInfo = getIngestToPredictParamInOne(bootStrapServers, zooKeeperConnect)
  val getServingWithKbParam: OnDemandStreamServingEngineInfo = getServingWithKbParam(bootStrapServers, zooKeeperConnect)
  val getKafkaIngestToPredictParamInOne: StreamToStreamEngineInfo = getKafkaIngestToPredictParamInOne(bootStrapServers, zooKeeperConnect)

  def getIngestionParam(
      bootStrapServers: String, zooKeeperConnect: String) = {
    val inHttpServerInfo = HttpServerInfo.newBuilder()
      .setPort(53002)
      .setIp("172.18.0.2")
      .build()
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.HttpServerReader")
      .setHttpServerReader(inHttpServerInfo)
      .build()

    val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group1")
      .setTopic("test")
      .build()
    val writer = StreamWriterInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build()

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder
          .setSparkArgs(SparkArgs.newBuilder()
              .setAppName("myApp")
              .setMaster("local[*]"))
          .build)
      .build()

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val ingestInfo = StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .setWriter(writer)
      .setRunner(runner)
      .build

    ingestInfo
  }

  def getDataParam(bootStrapServers: String, zooKeeperConnect: String) = {
    val inKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group1")
      .setTopic("test")
      .build

    val maxMinScalingInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(0)
      .setMax("0.5")
      .setMin("-0.5")
      .setWithMinMaxRange(true)
      .setMaxRealValue("50")
      .setMinRealValue("-20")
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingInfo)
      .build

    val transposeInfo = TransposeInfo.newBuilder()
      .setSelectedColumnName("temperature")
      .build
    val operator2 = StreamOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.operator.transformation.TransposeOperator")
      .setTranspose(transposeInfo)
      .build

    val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group2")
      .setTopic("test2")
      .build

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(inKafkaInfo)
      .build
    val writer = StreamWriterInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.reader.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(2)
            .setWindowSize(10)
            .setSlidingSize(1))

    val dataInfo = StreamToStreamEngineInfo.newBuilder()
        .setController(controller)
        .setReader(reader)
        .setWriter(writer)
        .addOperator(operator1)
        .addOperator(operator2)
        .setRunner(runner)
        .build

    dataInfo
  }

  def getPredictParam(bootStrapServers: String, zooKeeperConnect: String) = {
    val inKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group2")
      .setTopic("test2")
      .build
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(inKafkaInfo)
      .build

    val operator = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.analysis.SparkMLPredictOperator")
      .setMlStreamPredictor(
          SparkMLStreamPredictOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setModelPath(modelPath.replaceAll("\\\\", "/"))
          .build)

    val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group3")
      .setTopic("test3")
      .build
    val writer = StreamWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.reader.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkSessionOrStreamController")
      .setSparkSessionOrStreamController(
          SimpleBatchOrStreamControllerInfo.newBuilder()
          .setOperationPeriod(2))

    val predictInfo = StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator)
      .setWriter(writer)
      .setRunner(runner)
      .build
    predictInfo
  }

  def getDataToPredictParamInOne(bootStrapServers: String, zooKeeperConnect: String) = {

    val inKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group1")
      .setTopic("test")
      .build
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(inKafkaInfo)
      .build

    val maxMinScalingInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(0)
      .setMax("0.5")
      .setMin("-0.5")
      .setWithMinMaxRange(true)
      .setMaxRealValue("50")
      .setMinRealValue("-20")
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingInfo)
      .build

    val transposeInfo = TransposeInfo.newBuilder()
      .setSelectedColumnName("temperature")
      .build
    val operator2 = StreamOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.operator.transformation.TransposeOperator")
      .setTranspose(transposeInfo)
      .build

    val operator3 = StreamOperatorInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.operator.analysis.SparkMLPredictOperator")
      .setMlStreamPredictor(
          SparkMLStreamPredictOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setModelPath(modelPath.replaceAll("\\\\", "/"))
          .build)

    val maxMinScalingForDenormalInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(13)
      .setMax("50")
      .setMin("-20")
      .setWithMinMaxRange(true)
      .setMaxRealValue("0.5")
      .setMinRealValue("-0.5")
      .build
    val operator4 = StreamOperatorInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingForDenormalInfo)
      .build
    val operator5 = StreamOperatorInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(
          SelectColumnsInfo.newBuilder()
          .addSelectedColumnId(13)
          .build)

    val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group3")
      .setTopic("test3")
      .build

    val writer = StreamWriterInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.reader.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(2)
            .setWindowSize(10)
            .setSlidingSize(1))

    val ingestToPredicInfo = StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .setWriter(writer)
      .setRunner(runner)
      .build
    ingestToPredicInfo
  }

  def getIngestToPredictParamInOne(bootStrapServers: String, zooKeeperConnect: String) = {

    val inHttpServerInfo = HttpServerInfo.newBuilder()
      .setIp("0.0.0.0")
      .setPort(53002)
      .build()
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.HttpServerReader")
      .setHttpServerReader(inHttpServerInfo)
      .build()
    val selectedColumnsInfo = SelectColumnsInfo.newBuilder()
      .addSelectedColumnId(1)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(selectedColumnsInfo)
      .build
    val maxMinScalingInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(0)
      .setMax("0.5")
      .setMin("-0.5")
      .setWithMinMaxRange(true)
      .setMaxRealValue("50")
      .setMinRealValue("-20")
      .build
    val operator2 = StreamOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingInfo)
      .build
    val transposeInfo = TransposeInfo.newBuilder()
      .setSelectedColumnName("temperature")
      .build
    val operator3 = StreamOperatorInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.operator.transformation.TransposeOperator")
      .setTranspose(transposeInfo)
      .build
    val operator4 = StreamOperatorInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.operator.analysis.SparkMLPredictOperator")
      .setMlStreamPredictor(
          SparkMLStreamPredictOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setModelPath(modelPath.replaceAll("\\\\", "/"))
          .build)
    val maxMinScalingForDenormalInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(13)
      .setMax("50")
      .setMin("-20")
      .setWithMinMaxRange(true)
      .setMaxRealValue("0.5")
      .setMinRealValue("-0.5")
      .build
    val operator5 = StreamOperatorInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingForDenormalInfo)
      .build
    val operator6 = StreamOperatorInfo.newBuilder()
      .setId(7)
      .setPrevId(6)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(
          SelectColumnsInfo.newBuilder()
          .addSelectedColumnId(13)
          .build)
    val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group3")
      .setTopic("test3")
      .build
    val writer = StreamWriterInfo.newBuilder()
      .setId(8)
      .setPrevId(7)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(2)
            .setWindowSize(10)
            .setSlidingSize(1))

    val ingestToPredicInfo = StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .setWriter(writer)
      .setRunner(runner)
      .build

    ingestToPredicInfo
  }

  def getServingParam(bootStrapServers: String, zooKeeperConnect: String) = {
    val inKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group3")
      .setTopic("test3")
      .build
    val reader = OnDemandReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaOnDemandReader")
      .setKafkaOnDemandReader(inKafkaInfo)
      .build

    val runner = OnDemandRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.ServingRunner")
      .setWebRunner(
          WebserviceRunnerInfo.newBuilder()
            .setHost("0.0.0.0")
            .setPort(18080))
      .build

    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.ServingWithKbController")
      .setServingWithKbController(SimpleOnDemandControllerInfo.getDefaultInstance)

    val serviceInfo = OnDemandStreamServingEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .setRunner(runner)
      .build
    serviceInfo
  }

  def getServingWithKbParam(bootStrapServers: String, zooKeeperConnect: String) = {

    val inKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group3")
      .setTopic("test3")
      .build
    val reader = OnDemandReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaOnDemandReader")
      .setKafkaOnDemandReader(inKafkaInfo)
      .build

    val queryInfo = KbControlQueryInfo.newBuilder()
      .setUri("recommendDeviceControl")
      .setControlQuery(
          ControlQueryInfo.newBuilder()
          .setThingId("a1234")
          .setResourceId("temperature")
          .setTime("?")
          .addValues(
              Values.newBuilder()
              .setName("temperature")
              .setValue("?")))
    val operator = OnDemandOperatorInfo.newBuilder
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.service.ControlContextQueryOperator")
      .setKbControlQuery(queryInfo)
      .build

    val runner = OnDemandRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.ServingRunner")
      .setWebRunner(
          WebserviceRunnerInfo.newBuilder()
            .setHost("0.0.0.0")
            .setPort(18080))
      .build
    val controller = OnDemandControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.ServingWithKbController")
      .setServingWithKbController(
          SimpleOnDemandControllerInfo.getDefaultInstance)

    val serviceInfo = OnDemandStreamServingEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addOperator(operator)
      .setRunner(runner)
      .build
    serviceInfo
  }

  def getKafkaIngestToPredictParamInOne(bootStrapServers: String, zooKeeperConnect: String) = {

    val inKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group1")
      .setTopic("test1")
      .build
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(inKafkaInfo)
      .build
    val selectedColumnsInfo = SelectColumnsInfo.newBuilder()
      .addSelectedColumnId(1)
      .build
    val operator1 = StreamOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(selectedColumnsInfo)
      .build
    val maxMinScalingInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(0)
      .setMax("0.5")
      .setMin("-0.5")
      .setWithMinMaxRange(true)
      .setMaxRealValue("50")
      .setMinRealValue("-20")
      .build
    val operator2 = StreamOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingInfo)
      .build
    val transposeInfo = TransposeInfo.newBuilder()
      .setSelectedColumnName("temperature")
      .build
    val operator3 = StreamOperatorInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.operator.transformation.TransposeOperator")
      .setTranspose(transposeInfo)
      .build
    val operator4 = StreamOperatorInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.operator.analysis.SparkMLPredictOperator")
      .setMlStreamPredictor(
          SparkMLStreamPredictOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setModelPath(modelPath.replaceAll("\\\\", "/"))
          .build)
    val maxMinScalingForDenormalInfo = MinMaxScalingInfo.newBuilder()
      .addSelectedColumnId(13)
      .setMax("50")
      .setMin("-20")
      .setWithMinMaxRange(true)
      .setMaxRealValue("0.5")
      .setMinRealValue("-0.5")
      .build
    val operator5 = StreamOperatorInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(maxMinScalingForDenormalInfo)
      .build
    val operator6 = StreamOperatorInfo.newBuilder()
      .setId(7)
      .setPrevId(6)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectOperator")
      .setSelectColumns(
          SelectColumnsInfo.newBuilder()
          .addSelectedColumnId(13)
          .build)
    val outKafkaInfo = KafkaInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setGroupId("group3")
      .setTopic("test3")
      .build
    val writer = StreamWriterInfo.newBuilder()
      .setId(8)
      .setPrevId(7)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(outKafkaInfo)
      .build
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(2)
            .setWindowSize(10)
            .setSlidingSize(1))

    val ingestToPredicInfo = StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .setWriter(writer)
      .setRunner(runner)
      .build

    ingestToPredicInfo
  }

}
