package ksb.csle.examples.energy.serving.pipe

import scala.collection.JavaConversions._
import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.examples.energy.EnergyWorkflowLibrary

object HandleAggrDataStreamingEngine extends EnergyWorkflowLibrary {

  private[this] val modelPath: String = "hdfs://localhost:9000/energy/models/model_zone0"
  private[this] val bootStrapServers: String = "localhost:9092"
  private[this] val zooKeeperConnect: String = "localhost:2181"
  val input = System.getProperty("user.dir") + "/../examples"

  def main(args: Array[String]) =
    submit(appId, workflow)

  private def workflow = {
    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()
    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v1.0")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
          EngineInfo.newBuilder()
            .setId(1)
            .setPrevId(0)
            .setEngineNickName("Inference Engine")
            .setStreamJoinEngine(getIngestionToPredict)
            .build)
      .build()
  }

  def getIngestionToPredict: StreamJoinEngineInfo = {
    val jsonSamplePath = s"file:///$input/input/aggrData.csv"

    val inKafkaInfo = KafkaPipeReaderInfo.newBuilder()
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic("aggr-ingestion")
      .setFailOnDataLoss(false)
      .setSampleJsonPath(jsonSamplePath)
      .setAddTimestamp(true)
      .setTimestampName("time")
      .setWatermark("1 minutes")

    val reader = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName("ksb.csle.component.pipe.stream.reader.KafkaPipeReader")
      .setKafkaPipeReader(inKafkaInfo)
      .build

    val selectColumnInfo1 = SelectColumnsPipeInfo.newBuilder()
      .addColName("TIMESTAMP")
      .addColName("KIER/E3/Z208/OCCxMETER/OCC")
      .addColName("KIER/E3/Z208/EHPxMETER/P")
      .addColName("KIER/E3/Z208/LEDxMETER/P")
      .addColName("KIER/E3/Z208/OUTLETxG1xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG2xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG3xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG4xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG5xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG6xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG7xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG8xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG9xMETER/W")
      .addColName("KIER/E3/Z208/OUTLETxG10xMETER/W")
      .addColName("KIER/E3/Z208/INDOORxTEMPxHUMIxG1xMETER/TEMP")
      .addColName("KIER/E3/Z208/INDOORxTEMPxHUMIxG2xMETER/TEMP")
      .addColName("KIER/E3/Z208/INDOORxTEMPxHUMIxG1xMETER/HUMI")
      .addColName("KIER/E3/Z208/INDOORxTEMPxHUMIxG2xMETER/HUMI")
      .addColName("KIER/E3/Z208/INDOORxCO2xG1xMETER/CO2")
      .addColName("KIER/E3/Z208/INDOORxCO2xG2xMETER/CO2")
      .addColName("KIER/E3/Z208/OUTDOORxTEMPxHUMIxMETER/TEMP")
      .addColName("KIER/E3/Z208/OUTDOORxTEMPxHUMIxMETER/HUMI")
      .addColName("KIER/E3/Z208/OUTDOORxIRRxMETER/IRR")
      .addColName("ZoneID")
      .build

    val operator3 = StreamPipeOperatorInfo.newBuilder()
      .setId(3)
      .setPrevId(2)
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
          (Seq(14,15), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "KIER/E3/Z208/Temperature"))
      .addOperation(getColumnOperationInfo
          (Seq(24), ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE, "KIER/E3/Z208/Temperature").setValue(2.0))
      .build

    val operator4 = StreamPipeOperatorInfo.newBuilder()
      .setId(4)
      .setPrevId(3)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo1)
      .build

    val colOperatorInfo2 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq(16,17), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "KIER/E3/Z208/Humidity"))
      .addOperation(getColumnOperationInfo
          (Seq(25), ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE, "KIER/E3/Z208/Humidity").setValue(2.0))
      .build

    val operator5 = StreamPipeOperatorInfo.newBuilder()
      .setId(5)
      .setPrevId(4)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo2)
      .build

    val colOperatorInfo3 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq(18,19), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "KIER/E3/Z208/CO2"))
      .addOperation(getColumnOperationInfo
          (Seq(26), ColOperationPipeInfo.ColOperatorPipeType.COL_DIVIDEVALUE, "KIER/E3/Z208/CO2").setValue(2.0))
      .build

    val operator6 = StreamPipeOperatorInfo.newBuilder()
      .setId(6)
      .setPrevId(5)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo3)
      .build

    val colOperatorInfo4 = ColumnOperationPipeInfo.newBuilder()
      .addOperation(getColumnOperationInfo
          (Seq.range(4, 14), ColOperationPipeInfo.ColOperatorPipeType.COL_SUM, "KIER/E3/Z208/Plug_power"))
      .build

    val operator7 = StreamPipeOperatorInfo.newBuilder()
      .setId(7)
      .setPrevId(6)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.ColumnOperationOperator")
      .setColumOperations(colOperatorInfo4)
      .build

    val selectColumnInfo2 = SelectColumnsPipeInfo.newBuilder()
      .addColName("TIMESTAMP")
      .addColName("KIER/E3/Z208/OCCxMETER/OCC")
      .addColName("KIER/E3/Z208/EHPxMETER/P")
      .addColName("KIER/E3/Z208/LEDxMETER/P")
      .addColName("KIER/E3/Z208/Temperature")
      .addColName("KIER/E3/Z208/Humidity")
      .addColName("KIER/E3/Z208/CO2")
      .addColName("KIER/E3/Z208/Plug_power")
      .addColName("KIER/E3/Z208/OUTDOORxTEMPxHUMIxMETER/TEMP")
      .addColName("KIER/E3/Z208/OUTDOORxTEMPxHUMIxMETER/HUMI")
      .addColName("KIER/E3/Z208/OUTDOORxIRRxMETER/IRR")
      .addColName("ZoneID")

    val operator8 = StreamPipeOperatorInfo.newBuilder()
      .setId(8)
      .setPrevId(7)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SelectColumnsOperator")
      .setSelectColumns(selectColumnInfo2)
      .build

    val jdbcInfo = JDBCPipeInfo.newBuilder()
      .setAddress("localhost")
      .setDbName("energy")
      .setTableName("models")
      .setUserName("csleuser")
      .setPassword("csleuiux@2017.passwd")

    val operator9 = StreamPipeOperatorInfo.newBuilder()
      .setId(9)
      .setPrevId(8)
      .setClsName("ksb.csle.didentification.pipe.preprocessing.SparkMLPredictor")
      .setEnergyMLPredict(
        EnergyMLStreamPredictPipeOperatorInfo.newBuilder()
          .setClsNameForModel("org.apache.spark.ml.PipelineModel")
          .setZoneId("C1B1Z1R1")
          .setColumnName("ZoneID")
          .setJdbcInfo(jdbcInfo)
          .build)

    val checkpointPath1 = "hdfs://localhost:9000/energy/ckp/handleaggrdatastreaming/kafka/aggr"
    val outKafkaInfo = KafkaPipeWriterInfo.newBuilder()
      .setMode("append")
      .setBootStrapServers(bootStrapServers)
      .setZooKeeperConnect(zooKeeperConnect)
      .setTopic("topic_mldl")
      .setFailOnDataLoss(false)
      .setTrigger("1 seconds")
      .setCheckpointLocation(checkpointPath1)
      .build
    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(10)
      .setPrevId(9)
      .setClsName("ksb.csle.component.pipe.stream.writer.KafkaPipeWriter")
      .setKafkaPipeWriter(outKafkaInfo)
      .build

//    val csvFilePath = s"file:///$inputDir/input/rawData"
    val csvFilePath = s"hdfs://localhost:9000/energy/data/raw/rawData"
    val fileInfo = FileInfo.newBuilder
      .setFileType(FileInfo.FileType.CSV)
      .setDelimiter(",")
      .setHeader(true)
      .addFilePath(csvFilePath)
      .build()
    val checkpointPath2 = "hdfs://localhost:9000/energy/ckp/handleaggrdatastreaming/file/aggr"
    val outFileInfo = FilePipeWriterInfo.newBuilder()
      .setFileInfo(fileInfo)
      .setCheckpointLocation(checkpointPath2)
      .setMode("append") // Data source csv does not support Complete output mode
      .setTrigger("10 seconds")

    val writer2 = StreamPipeWriterInfo.newBuilder()
      .setId(11)
      .setPrevId(10)
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

    StreamJoinEngineInfo.newBuilder()
      .setController(controller)
      .addReader(reader)
      .addOperator(operator3)
      .addOperator(operator4)
      .addOperator(operator5)
      .addOperator(operator6)
      .addOperator(operator7)
      .addOperator(operator8)
      .addOperator(operator9)
      .addWriter(writer1)
      .addWriter(writer2)
      .setRunner(runner)
      .build
  }

}
