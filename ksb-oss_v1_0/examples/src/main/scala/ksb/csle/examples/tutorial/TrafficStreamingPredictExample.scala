package ksb.csle.examples.tutorial

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}
import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.Message
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.StreamControlProto._
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.tools.client._

object TrafficStreamingPredictExample extends Logging {
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
      .setBatch(false)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(
          RunType.newBuilder()
            .setId(1)
            .setPeriodic(Periodic.ONCE))
      .addRuntypes(
          RunType.newBuilder()
            .setId(2)
            .setPeriodic(Periodic.ONCE))
      .addRuntypes(
          RunType.newBuilder()
            .setId(3)
            .setPeriodic(Periodic.ONCE))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("StreamProcessingEngine")
            .setStreamJoinEngine(engin1Param))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(2)
            .setPrevId(1)
            .setEngineNickName("StreamProcessing2Engine")
            .setStreamJoinEngine(engin2Param))
      .addEngines(
          EngineInfo.newBuilder()
            .setId(3)
            .setPrevId(2)
            .setEngineNickName("PredictEngine")
            .setStreamToStreamEngine(predicParam))
      .build()
  }
  

  private def engin1Param = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")

    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "traffic"
    val kafkaTopic = "traffic"

    //python kangnam_producer.py /home/csle/ksb-csle/examples/input/201601_kangnam_orgarnized_new.csv localhost:9092 traffic 0.01
    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.pipe.stream.reader.KafkaPipeReader")
      .setKafkaPipeReader(
          KafkaPipeReaderInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setTopic(kafkaTopic)
            .setFailOnDataLoss(false)
            .setSampleJsonPath(s"file:///home/csle/ksb-csle/examples/input/trafficStreamingSplitSample.json")
            .setAddTimestamp(false)
            .setTimestampName("PRCS_DATE")
            .setWatermark("2 minutes"))     

    val writer = StreamPipeWriterInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(
          KafkaPipeWriterInfo.newBuilder()
          .setMode("append")
          .setBootStrapServers(kafkaServer)
          .setZooKeeperConnect(kafkaZookeeper)
          .setCheckpointLocation(s"file:///tmp/kangnam/checkpoint/kafka1")
          .setTopic("traffic_output1")
          .setFailOnDataLoss(true)
          .setTrigger("5 seconds"))   

    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.pipe.stream.operator.GroupByOperator")
      .setGroupby(
          GroupbyPipeInfo.newBuilder()
          .setTimeColName("PRCS_DATE")
          .addKeyColName("LINK_ID")
          .addValColName("PRCS_SPD")
          .setGroupby(GroupbyPipeInfo.GroupbyOp.AVG)
          .setWindow(
              Window.newBuilder()
              .setKey("PRCS_DATE")
              .setWindowLength("1 minutes")
              .setSlidingInterval("30 seconds"))
          .build())
      .build

    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.pipe.stream.operator.SelectColumnsPipeOperator")
      .setSelectColumns(
          SelectColumnsPipeInfo.newBuilder()
            .addColName("LINK_ID")
            .addColName("window.start")
            .addColName("PRCS_SPD")
       )

    val operator3 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.pipe.stream.operator.RenameColumnsPipeOperator")
      .setRenameCol(
          RenameColumnsPipeInfo.newBuilder()
            .addSelectedColumn(
                SelectedColumnInfo.newBuilder()
                  .setSelectedColIndex(1)
                  .setNewColName("PRCS_DATE")
                  .setNewFieldType(FieldType.STRING))
      )

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addWriter(writer)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .setRunner(runner)
      .build()
  }

  private def engin2Param = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")

    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "traffic"
    val kafkaTopic = "traffic_output1"

    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.pipe.stream.reader.KafkaPipeReader")
      .setKafkaPipeReader(
          KafkaPipeReaderInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setTopic(kafkaTopic)
            .setFailOnDataLoss(false)
            .setSampleJsonPath(s"file:///home/csle/ksb-csle/examples/input/trafficStreamingSplitSample.json")
            .setAddTimestamp(false)
            .setTimestampName("PRCS_DATE")
            .setWatermark("2 minutes"))

    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(
          KafkaPipeWriterInfo.newBuilder()
          .setMode("append")
          .setBootStrapServers(kafkaServer)
          .setZooKeeperConnect(kafkaZookeeper)
          .setCheckpointLocation(s"file:///tmp/kangnam/checkpoint/kafka2")
          .setTopic("traffic_output2")
          .setFailOnDataLoss(true)
          .setTrigger("5 seconds"))

//    val writer2 = StreamPipeWriterInfo.newBuilder()
//      .setId(6)
//      .setPrevId(5)
//      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
//      .setKafkaPipeWriter(
//          KafkaPipeWriterInfo.newBuilder()
//          .setMode("append")
//          .setBootStrapServers(kafkaServer)
//          .setZooKeeperConnect(kafkaZookeeper)
//          .setCheckpointLocation(s"file:///tmp/kangnam/checkpoint/kafka2_copy")
//          .setTopic("traffic_output2_copy")
//          .setFailOnDataLoss(true)
//          .setTrigger("5 seconds"))

    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(1)
      .setId(2)
      .setClsName("ksb.csle.component.pipe.stream.operator.GroupByOperator")
      .setGroupby(
          GroupbyPipeInfo.newBuilder()
          .setTimeColName("PRCS_DATE")
          .addKeyColName("LINK_ID")
          .addValColName("PRCS_SPD")
          .setGroupby(GroupbyPipeInfo.GroupbyOp.AVG)
          .setWindow(
              Window.newBuilder()
              .setKey("PRCS_DATE")
              .setWindowLength("5 minutes")
              .setSlidingInterval("5 minutes"))
          .build())
      .build

    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.pipe.stream.operator.SelectColumnsPipeOperator")
      .setSelectColumns(
          SelectColumnsPipeInfo.newBuilder()
            .addColName("LINK_ID")
            .addColName("window.start")
            .addColName("PRCS_SPD")
       )

    val operator3 = StreamPipeOperatorInfo.newBuilder()
      .setPrevId(3)
      .setId(4)
      .setClsName("ksb.csle.component.pipe.stream.operator.RenameColumnsPipeOperator")
      .setRenameCol(
          RenameColumnsPipeInfo.newBuilder()
            .addSelectedColumn(
                SelectedColumnInfo.newBuilder()
                  .setSelectedColIndex(1)
                  .setNewColName("PRCS_DATE")
                  .setNewFieldType(FieldType.STRING))
      )

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addWriter(writer1)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .setRunner(runner)
      .build()
  }

  private def predicParam = {
    val masterMode = ConfigUtils.getConfig().envOrElseConfig(
      "servers.spark.master")

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.controller.SparkStreamController")
      .setSparkStreamController(
          SparkStreamControllerInfo.newBuilder()
            .setOperationPeriod(5)
            .setWindowSize(170*8 + 160)
            .setSlidingSize(170))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(SparkRunnerInfo.newBuilder()
          .setSparkArgs(SparkArgs.newBuilder()
              .setDriverMemory("1g")
              .setExecuterMemory("1g")))

    val kafkaServer = "localhost:9092"
    val kafkaZookeeper = "localhost:2181"
    val kafkaGroupId = "traffic"
    val kafkaTopic = "traffic_output2"
    val reader = StreamReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.reader.KafkaReader")
      .setKafkaReader(
          KafkaInfo.newBuilder()
            .setBootStrapServers(kafkaServer)
            .setZooKeeperConnect(kafkaZookeeper)
            .setGroupId(kafkaGroupId)
            .setTopic(kafkaTopic))

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
      .setClsName("ksb.csle.component.operator.reduction.GroupByFilterOperator")
      .setGroupbyFilter(GroupbyFilterInfo.newBuilder()
          .setKeyColName("PRCS_DATE")
          .setGroupby(GroupbyFilterInfo.GroupbyOp.COUNT)
          .setCondition(GroupbyFilterInfo.Condition.EQUAL)
          .setValue(170)
          )
      .build()

    val operator3 = StreamOperatorInfo.newBuilder()
      .setPrevId(2)
      .setId(3)
      .setClsName("ksb.csle.component.operator.transformation.MinMaxScalingOperator")
      .setMinMaxScaling(MinMaxScalingInfo.newBuilder()
          .addSelectedColumnId(2) // PRCS_SPD
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
          .setSelectedColumnId(1) // LINK_ID
          .setGroupByColumn("0") // PRCS_DATE
          .setValueColumn("2") // PRCS_SPD
          .setMethod(PivotInfo.Method.AVG))
      .build()

    val columnIdPath = s"file:///home/csle/ksb-csle/examples/input/traffic_kangnam_cols.txt"
    val operator5 = StreamOperatorInfo.newBuilder()
      .setPrevId(4)
      .setId(5)
      .setClsName("ksb.csle.component.operator.reduction.ColumnSelectWithFileOperator")
      .setSelectColumnsWithFile(
          SelectColumnsWithFileInfo.newBuilder()
          .setColumnIdPath(columnIdPath))
      .build()

    val operator6 = StreamOperatorInfo.newBuilder()
      .setPrevId(5)
      .setId(6)
      .setClsName("ksb.csle.component.operator.integration.VectorAssembleColumnAddOperator")
      .setAddVectorAssembleColumn(AddVectorAssembleColumnInfo.newBuilder()
          .setVectorAssembleColumnName("in1"))
      .build()

    val operator7 = StreamOperatorInfo.newBuilder()
      .setPrevId(6)
      .setId(7)
      .setClsName("ksb.csle.component.operator.transformation.FlattenOperator")
      .setFlatten(FlattenInfo.newBuilder()
          .setColumnName("in1"))

    val workingDirPath = System.getProperty("user.dir")
    val modelBasePath = s"file:///home/csle/ksb-csle/examples/models/kangnam/model"
    val operator8 = StreamOperatorInfo.newBuilder()
      .setPrevId(7)
      .setId(8)
      .setClsName("ksb.csle.component.operator.analysis.TensorflowPredictOperator")
      .setTfPredictor(TensorflowPredictorInfo.newBuilder()
          .setModelServerUri(modelBasePath)
          .setModelName("kangnam_traffic")
          .setSignatureName("predict_speed"))
      .build()

    val writer = StreamWriterInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.writer.KafkaWriter")
      .setKafkaWriter(KafkaInfo.newBuilder()
          .setBootStrapServers(kafkaServer)
          .setZooKeeperConnect(kafkaZookeeper)
          .setGroupId(kafkaGroupId)
          .setTopic("kangnam_output"))


    StreamToStreamEngineInfo.newBuilder()
      .setController(controller)
      .setRunner(runner)
      .setReader(reader)
      .setWriter(writer)
      .addOperator(operator1)
      .addOperator(operator2)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .addOperator(operator7)
      .addOperator(operator8)
      .build()
  }

}
