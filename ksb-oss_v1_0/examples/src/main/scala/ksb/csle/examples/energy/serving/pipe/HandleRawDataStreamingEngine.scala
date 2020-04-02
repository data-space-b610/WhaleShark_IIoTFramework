package ksb.csle.examples.energy.serving.pipe

import scala.collection.JavaConversions._
import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.examples.energy.EnergyWorkflowLibrary

object HandleRawDataStreamingEngine extends EnergyWorkflowLibrary {

  private[this] val modelPath: String = "hdfs://csle1:9000/energy/models/model_zone0"
  private[this] val bootStrapServers: String = "csle1:9092"
  private[this] val zooKeeperConnect: String = "csle1:2181"
  private[this] val inputDir = System.getProperty("user.dir") + "/../examples"
  val kafkaReaderClassName = "ksb.csle.component.pipe.stream.reader.KafkaPipeReader"

  def main(args: Array[String]) =
    submit(appId, workflow)

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
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("Aggregate streaming data of 10 Sensors")
            .setStreamJoinEngine(getRawStreamingData)
            .build)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(2)
            .setPrevId(1)
            .setEngineNickName("Handle aggregated streaming data")
            .setStreamJoinEngine(handleAggregateStreaming)
            .build)
      .build()
  }

  def getRawStreamingData: StreamJoinEngineInfo = {
    def makeKafkaPipeReaderInfo(
        topicIndex: Int):KafkaPipeReaderInfo = {
    val jsonSamplePath = s"hdfs://localhost:9000/datasets/input/streamingRawJson.csv"
      KafkaPipeReaderInfo.newBuilder()
        .setBootStrapServers(bootStrapServers)
        .setZooKeeperConnect(zooKeeperConnect)
        .setTopic("topic_"+topicIndex)
        .setFailOnDataLoss(false)
        .setSampleJsonPath(jsonSamplePath)
        .setAddTimestamp(false)
        .setTimestampName("TIMESTAMP")
        .setWatermark("5 seconds")
        .build
    }

//    val sensorList = Seq.range(54, 63) ++ Seq(24,52,64,67,70,73,76,79,82,85,88,91,93)
    val sensorList = Seq.range(59, 63) ++ Seq(24,52,54,56,64,93)
    val kafkaReaders = sensorList.map(x => {
      val kafkaInfo = makeKafkaPipeReaderInfo(x)
      StreamPipeReaderInfo.newBuilder()
        .setId(x)
        .setPrevId(x-1)
        .setClsName(kafkaReaderClassName)
        .setKafkaPipeReader(kafkaInfo)
        .build
    })

    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setId(94)
      .setPrevId(93)
      .setClsName("ksb.csle.component.pipe.stream.operator.StreamJoinOperator")
      .setJoin(
          JoinPipeInfo.newBuilder()
          .setKey("TIMESTAMP")
          .addJoinColumns("VALUE_STRING")
          .build())

    // the column start at index 0. check the file
    val columnNamePath = s"hdfs://localhost:9000/datasets/input/sensorTmp.csv"
    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setId(95)
      .setPrevId(94)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnsRenamerOperator")
      .setRenameColumns(
          RenameColumnsInfo.newBuilder()
          .setColNameFromPath(columnNamePath)
          .build())

    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.component.pipe.stream.writer.ConsolePipeWriter")
      .setConsolePipeWriter(
          ConsolePipeWriterInfo.newBuilder()
          .setMode("append")
          .setTrigger("5 seconds"))

    val aggrDataSendToNext = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "ingestion", false, "5 seconds", "ckp/energy/kafka")
    val writer2 = StreamPipeWriterInfo.newBuilder()
      .setId(101)
      .setPrevId(100)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(aggrDataSendToNext)
      .build

    val csvFilePath = "hdfs://csle1:9000/datasets/output/energy/raw/"
    val outFileInfo = FilePipeWriterInfo.newBuilder()
      .setCheckpointLocation("ckp/energy/hdfs")
      .setTrigger("5 seconds")
      .setFileInfo(FileInfo.newBuilder
        .setFileType(FileInfo.FileType.CSV)
        .setDelimiter(",")
        .setHeader(true)
        .addFilePath(csvFilePath))
    val writer3 = StreamPipeWriterInfo.newBuilder()
      .setId(100)
      .setPrevId(99)
      .setClsName("ksb.csle.component.pipe.stream.writer.FilePipeWriter")
      .setFilePipeWriter(outFileInfo)
      .build

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val rawStreamJoinEngine = StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addOperator(operator1)
      .addOperator(operator2)
      .addWriter(writer1)
      .addWriter(writer2)
      .addWriter(writer3)
      .setRunner(runner)

    kafkaReaders.map(reader => rawStreamJoinEngine.addReader(reader))
    rawStreamJoinEngine.build
  }

  def handleAggregateStreaming: StreamJoinEngineInfo = {
    val jsonSamplePath = s"hdfs://localhost:9000/datasets/input/aggr13Json.csv"
    val inKafkaInfo = KafkaPipeReaderInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic("ingestion")
      .setFailOnDataLoss(false)
      .setSampleJsonPath(jsonSamplePath)
      .setAddTimestamp(false)

    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.pipe.stream.reader.KafkaPipeReader")
      .setKafkaPipeReader(inKafkaInfo)
      .build

    val selectColumnInfo = SelectColumnsPipeInfo.newBuilder()
//      .addColName("TIMESTAMP")
//      .addColName("KIER/E3/Z208/OCCxMETER/OCC")
      .addColName("KIER/E3/Z208/EHPxMETER/P")
      .addColName("KIER/E3/Z208/LEDxMETER/P")
      .addColName("KIER/E3/Z208/LEDxMETER/P")
      .addColName("KIER/E3/Z208/Humidity")
      .addColName("KIER/E3/Z208/CO2")
      .addColName("KIER/E3/Z208/Plug_power")
      .addColName("KIER/E3/Z208/OUTDOORxTEMPxHUMIxMETER/TEMP")
      .addColName("KIER/E3/Z208/OUTDOORxTEMPxHUMIxMETER/HUMI")
      .addColName("KIER/E3/Z208/OUTDOORxIRRxMETER/IRR")

    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setId(102)
      .setPrevId(101)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SelectColumnsOperator")
      .setSelectColumns(selectColumnInfo)
      .build

    val modelPath = "hdfs://localhost:9000/datasets/models/rfModel/"
    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setId(103)
      .setPrevId(102)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SparkMLPredictor")
      .setEnergyMLPredict(
        EnergyMLStreamPredictPipeOperatorInfo.newBuilder()
          .setClsNameForModel(modelPath)
          .setZoneId("Z208")
          .build)

    val csvFilePath = s"hdfs://csle1:9000/datasets/output/energy/mlData/"
    val outFileInfo = FilePipeWriterInfo.newBuilder()
      .setCheckpointLocation("ckp/energy/hdfs2")
      .setTrigger("5 seconds")
      .setFileInfo(FileInfo.newBuilder
        .setFileType(FileInfo.FileType.CSV)
        .setDelimiter(",")
        .setHeader(true)
        .addFilePath(csvFilePath))
    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(110)
      .setPrevId(109)
      .setClsName("ksb.csle.component.pipe.stream.writer.FilePipeWriter")
      .setFilePipeWriter(outFileInfo)
      .build

    val kafkaCheckpointPath = "ckp/energy/kafka2"
    val toInference = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "topic_mldl", false, "5 seconds", kafkaCheckpointPath)
    val writer2 = StreamPipeWriterInfo.newBuilder()
      .setId(111)
      .setPrevId(110)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(toInference)
      .build

    val writer3 = StreamPipeWriterInfo.newBuilder()
      .setId(112)
      .setPrevId(111)
      .setClsName("ksb.csle.component.pipe.stream.writer.ConsolePipeWriter")
      .setConsolePipeWriter(
          ConsolePipeWriterInfo.newBuilder()
          .setMode("append")
          .setTrigger("5 seconds"))

    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.newBuilder()
        .setSparkArgs(sparkCluster))
      .build

    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val streamJoinEngine = StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addOperator(operator1)
      .addOperator(operator2)
      .addWriter(writer1)
      .addWriter(writer2)
      .addWriter(writer3)
      .setRunner(runner)

    streamJoinEngine.build
  }

}
