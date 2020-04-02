package ksb.csle.examples.energy.serving.pipe

import scala.collection.JavaConversions._
import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.examples.energy.EnergyWorkflowLibrary

object PipeEngine extends EnergyWorkflowLibrary {

  private[this] val modelPath: String = "hdfs://localhost:9000/energy/models/model_zone0"
  private[this] val bootStrapServers: String = "localhost:9092"
  private[this] val zooKeeperConnect: String = "localhost:2181"
  private[this] val inputDir = System.getProperty("user.dir") + "/../examples"
//  private[this] val jsonSamplePath = s"file:///$inputDir/input/jsonSample.csv"
  private[this] val jsonSamplePath = s"file:///$inputDir/input/jsonSample2.csv"
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
            .setId(4)
            .setPeriodic(Periodic.ONCE))
//      .addEngines(
//          EngineInfo.newBuilder()
//            .setId(2)
//            .setPrevId(1)
//            .setEngineNickName("Input: Kafka, Output: HBASE, HDFS, KAFKA Engine")
//            .setStreamJoinEngine(getIngestionData)
//            .build)
//      .addEngines(
//          EngineInfo.newBuilder()
//            .setId(3)
//            .setPrevId(2)
//            .setEngineNickName("Input: Kafka, Output: HBASE Engine")
//            .setStreamJoinEngine(saveToHbase)
//            .build)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(4)
            .setPrevId(3)
            .setEngineNickName("Input: Kafka, Output: HDFS Engine")
            .setStreamJoinEngine(saveToHdfs)
            .build)
//      .addEngines(
//          EngineInfo.newBuilder()
//            .setId(5)
//            .setPrevId(4)
//            .setEngineNickName("Input: Kafka, Output: KAFKA Engine")
//            .setStreamJoinEngine(sendToInference)
//            .build)
      .build()
  }

  def getIngestionData: StreamJoinEngineInfo = {
    val inKafkaInfo = KafkaPipeReaderInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic("ingestion")
      .setAddTimestamp(false)
      .setFailOnDataLoss(false)
      .setSampleJsonPath(jsonSamplePath)
//      .setTimestampName("TIMESTAMP")
//      .setWatermark("5 minutes")

    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(kafkaReaderClassName)
      .setKafkaPipeReader(inKafkaInfo)
      .build

    val toHbase = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "toHbase", false, "1 second", "toHbase/kafka/checkpoint")
//        zooKeeperConnect, "toHbase", "TOPIC_ID", false, "toHbase/kafka/checkpoint")
    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(toHbase)
      .build

    val toHdfs = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "toHdfs", false, "1 second", "toHdfs/kafka/checkpoint")
//        zooKeeperConnect, "toHdfs", "TOPIC_ID", false, "toHdfs/kafka/checkpoint")
    val writer2 = StreamPipeWriterInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(toHdfs)
      .build

    val toInference = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "toInference", false, "1 second", "toInference/kafka/checkpoint")
//        zooKeeperConnect, "toInference", "TOPIC_ID", false, "toInference/kafka/checkpoint")
    val writer3 = StreamPipeWriterInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(toInference)
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

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
//      .addWriter(writer1)
      .addWriter(writer2)
//      .addWriter(writer3)
      .setRunner(runner)
      .build
  }

  def saveToHbase: StreamJoinEngineInfo = {
    val inKafkaInfo = KafkaPipeReaderInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic("toHbase")
      .setFailOnDataLoss(false)
      .setSampleJsonPath(jsonSamplePath)
      .setAddTimestamp(false)
      .setTimestampName("TIMESTAMP")
      .setWatermark("5 minutes")
    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(kafkaReaderClassName)
      .setKafkaPipeReader(inKafkaInfo)
      .build

    val hbaseInfo = HBasePipeWriterInfo.newBuilder()
      .setMode("update")
      .setTrigger("10 seconds")
      .setCheckpointLocation("toHbase/hbase/checkpoint")
      .setCatalog("data_table")
    val writer = StreamPipeWriterInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.component.pipe.stream.writer.HBasePipeWriter")
      .setHbasePipeWriter(hbaseInfo)
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

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addWriter(writer)
      .setRunner(runner)
      .build
  }

  def saveToHdfs: StreamJoinEngineInfo = {
    def makeKafkaPipeReaderInfo(
        topicIndex: Int):KafkaPipeReaderInfo = {
      KafkaPipeReaderInfo.newBuilder()
        .setBootStrapServers(bootStrapServers)
        .setZooKeeperConnect(zooKeeperConnect)
        .setTopic("topic_"+topicIndex)
        .setFailOnDataLoss(false)
        .setSampleJsonPath(jsonSamplePath)
        .setAddTimestamp(false)
        .setTimestampName("TIMESTAMP")
        .setWatermark("1 minute")
        .build
    }

//    val kafkaReaders = Seq.range(0 , 20).map(x => {
    val kafkaReaders = Seq.range(2, 94).map(x => {
      val kafkaIno = makeKafkaPipeReaderInfo(x)
      StreamPipeReaderInfo.newBuilder()
        .setId(x)
        .setPrevId(x-1)
        .setClsName(kafkaReaderClassName)
        .setKafkaPipeReader(kafkaIno)
        .build
    })

    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setId(94)
      .setPrevId(93)
      .setClsName("ksb.csle.component.operator.pipe.stream.operator.StreamJoinOperator")
      .setJoin(
          JoinPipeInfo.newBuilder()
          .setKey("TIMESTAMP")
          .addJoinColumns("VALUE_STRING")
          .build())

    val columnNamePath = s"file:///$inputDir/input/topicHbase.csv"
    val operator2 = StreamPipeOperatorInfo.newBuilder()
      .setId(95)
      .setPrevId(94)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnsRenamer")
      .setRenameColumns(
          RenameColumnsInfo.newBuilder()
          .setColNameFromPath(columnNamePath)
          .build())

//    val aggrInfo = AggregateTimePipeInfo.newBuilder()
//      .setTimeColumn("TIMESTAMP")
//      .setAggrType(AggregateTimePipeInfo.AggregateType.AGGR_AVERAGE)
//      .setWindow(Window.newBuilder()
//          .setKey("TIMESTAMP")
//          .setWindowLength("2 minutes")
//          .setSlidingInterval("1 minutes"))
//      .build()
//    val operator3 = StreamPipeOperatorInfo.newBuilder()
//      .setId(96)
//      .setPrevId(95)
//      .setClsName("ksb.csle.didentification.pipe.preprocessing.TimeAggregator")
//      .setAggregateTime(aggrInfo)
//      .build()

    val selectColumnInfo1 = SelectColumnsPipeInfo.newBuilder()
      .addColName("TIMESTAMP")
      .addColName("Floor2_Occupancy")
      .addColName("Floor2_EHP_power")
      .addColName("Floor2_LED_power")
      .addColName("Floor2_Smartplug1_Power")
      .addColName("Floor2_Smartplug2_Power")
      .addColName("Floor2_Smartplug3_Power")
      .addColName("Floor2_Smartplug4_Power")
      .addColName("Floor2_Smartplug5_Power")
      .addColName("Floor2_Smartplug6_Power")
      .addColName("Floor2_Smartplug7_Power")
      .addColName("Floor2_Smartplug8_Power")
      .addColName("Floor2_Smartplug9_Power")
      .addColName("Floor2_Smartplug10_Power")
      .addColName("Floor2_Temperature_1")
      .addColName("Floor2_Temperature_2")
      .addColName("Floor2_Humidity_1")
      .addColName("Floor2_Humidity_2")
      .addColName("Floor2_Co2_1")
      .addColName("Floor2_Co2_2")
      .addColName("Outdoor_Temperature")
      .addColName("Outdoor_Humidity")
      .addColName("Outdoor_Irradiation")
      .build

    val operator4 = StreamPipeOperatorInfo.newBuilder()
      .setId(97)
      .setPrevId(96)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SelectColumnsOperator")
      .setSelectColumns(selectColumnInfo1)
      .build

    def getColumnOperationInfo(
        colIds: Seq[Int],
        operator: ColOperationPipeInfo.ColOperatorPipeType,
        name: String): ColOperationPipeInfo.Builder = {
      val operationInfo = ColOperationPipeInfo.newBuilder()
        .setOperator(operator)
        .setMadeColumnName(name)

      colIds.map(x => operationInfo.addColId(x))
      (operationInfo)
    }

    val colOperatorInfo1 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq(14,15), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "Temperature"))
      .addOperation(getColumnOperationInfo
          (Seq(23), ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE, "Temperature").setValue(2.0))
      .build

    val operator5 = StreamPipeOperatorInfo.newBuilder()
      .setId(98)
      .setPrevId(97)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo1)
      .build

    val colOperatorInfo2 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq(16,17), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "Humidity"))
      .addOperation(getColumnOperationInfo
          (Seq(24), ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE, "Humidity").setValue(2.0))
      .build

    val operator6 = StreamPipeOperatorInfo.newBuilder()
      .setId(99)
      .setPrevId(98)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo2)
      .build

    val colOperatorInfo3 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq(18,19), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "CO2"))
      .addOperation(getColumnOperationInfo
          (Seq(25), ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE, "CO2").setValue(2.0))
      .build

    val operator7 = StreamPipeOperatorInfo.newBuilder()
      .setId(100)
      .setPrevId(99)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo3)
      .build

    val colOperatorInfo4 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq.range(4, 14), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "Plug_power"))
      .build

    val operator8 = StreamPipeOperatorInfo.newBuilder()
      .setId(101)
      .setPrevId(100)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo4)
      .build

    val selectColumnInfo2 = SelectColumnsPipeInfo.newBuilder()
      .addColName("TIMESTAMP")
      .addColName("Floor2_Occupancy")
      .addColName("Floor2_EHP_power")
      .addColName("Floor2_LED_power")
      .addColName("Temperature")
      .addColName("Humidity")
      .addColName("CO2")
      .addColName("Plug_power")
      .addColName("Outdoor_Temperature")
      .addColName("Outdoor_Humidity")
      .addColName("Outdoor_Irradiation")

    val operator9 = StreamPipeOperatorInfo.newBuilder()
      .setId(102)
      .setPrevId(101)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SelectColumnsOperator")
      .setSelectColumns(selectColumnInfo2)
      .build

    val jdbcInfo = JDBCPipeInfo.newBuilder()
      .setAddress("localhost")
      .setDbName("energy")
      .setTableName("models")
      .setUserName("csleuser")
      .setPassword("csleuiux@2017.passwd")
    val operator10 = StreamPipeOperatorInfo.newBuilder()
      .setId(103)
      .setPrevId(102)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SparkMLPredictor")
      .setEnergyMLPredict(
        EnergyMLStreamPredictPipeOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setZoneId("C1B1Z1R1")
          .setColumnName("ZoneID")
          .setJdbcInfo(jdbcInfo)
          .build)

    val csvFilePath = s"hdfs://localhost:9000/energy/data/hdfs/"
    val outFileInfo = FilePipeWriterInfo.newBuilder()
      .setCheckpointLocation("toHdfs/hdfs/checkpoint")
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

    val toInference = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "inference2", false, "5 seconds", "toResult/kafka/checkpoint")
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
      .addOperator(operator1)
      .addOperator(operator2)
//      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .addOperator(operator7)
      .addOperator(operator8)
      .addOperator(operator9)
      .addOperator(operator10)
//      .addWriter(writer1)
      .addWriter(writer2)
      .addWriter(writer3)
      .setRunner(runner)
    kafkaReaders.map(reader => streamJoinEngine.addReader(reader))

    streamJoinEngine.build
  }

  def sendToInference: StreamJoinEngineInfo = {
    val inKafkaInfo = KafkaPipeReaderInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic("toInference")
      .setFailOnDataLoss(false)
      .setSampleJsonPath(jsonSamplePath)
      .setAddTimestamp(false)
      .setTimestampName("TIMESTAMP")
      .setWatermark("4 minutes")
    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(kafkaReaderClassName)
      .setKafkaPipeReader(inKafkaInfo)
      .build

    val groupByInfo = GroupbyPipeInfo.newBuilder()
      .setTimeColName("TIMESTAMP")
      .addKeyColName("TOPIC_ID")
      .addValColName("VALUE_STRING")
      .setGroupby(GroupbyPipeInfo.GroupbyOp.AVG)
      .setWindow(Window.newBuilder()
          .setKey("TIMESTAMP")
          .setWindowLength("2 minutes")
          .setSlidingInterval("1 minutes"))
    val operator1 = StreamPipeOperatorInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.operator.pipe.stream.operator.GroupByOperator")
      .setGroupby(groupByInfo)
      .build()

    val toInference = makeKafkaPipeWriter("append", bootStrapServers,
        zooKeeperConnect, "inference", false, "1 second", "toResult/kafka/checkpoint")
//        zooKeeperConnect, "inference", "TOPIC_ID", false, "toResult/kafka/checkpoint")
    val writer = StreamPipeWriterInfo.newBuilder()
      .setId(7)
      .setPrevId(6)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(toInference)
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

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addOperator(operator1)
      .addWriter(writer)
      .setRunner(runner)
      .build
  }

}
