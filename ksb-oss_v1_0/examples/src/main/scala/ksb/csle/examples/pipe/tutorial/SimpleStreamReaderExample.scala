package ksb.csle.examples.pipe.tutorial

import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.examples.SimpleClient

/**
 * Object to create workflow senario as a protobuf message WorkflowInfo.
 * See {@link WorkflowInfo}. For now, this is used for test of basic workflow senario.
 *  This will be replaced with Workflow editor.
 *
 * TODO: Support graphical user interface for easy workflow editing.
 */
object SimpleStreamReaderExample extends App {
  val appId: String = "SimpleStreamReaderExample"

  SimpleClient.submit(workflow)

  private def workflow = {
    val readerClassName = "ksb.csle.component.pipe.stream.reader.FilePipeReader"

    val reader1 = StreamPipeReaderInfo.newBuilder()
      .setId(1)
      .setPrevId(0)
      .setClsName(readerClassName)
      .setFilePipeReader(
        FilePipeReaderInfo.newBuilder()
          .setFilePath("file:///home/csle/workspace/csle_20180212/gy/input1")
          .setFileType(FilePipeReaderInfo.FileType.CSV)
          .setTimeColName("time")
          .setWatermark("10 seconds")
          .addField(
            FieldInfo.newBuilder()
              .setKey("name")
              .setType(FieldInfo.FieldType.STRING)
              .build())
          .addField(
            FieldInfo.newBuilder()
              .setKey("age")
              .setType(FieldInfo.FieldType.INTEGER)
              .build())
          .addField(
            FieldInfo.newBuilder()
              .setKey("time")
              .setType(FieldInfo.FieldType.TIMESTAMP)
              .build())
        )

    val writer1 = StreamPipeWriterInfo.newBuilder()
      .setId(2)
      .setPrevId(1)
      .setClsName("ksb.csle.component.pipe.stream.writer.ConsolePipeWriter")
      .setConsolePipeWriter(
          ConsolePipeWriterInfo.newBuilder()
          .setMode("append")
          .setTrigger("2 seconds"))
    val runner = StreamRunnerInfo.newBuilder()
      .setClsName("ksb.csle.component.runner.SimpleSparkRunner")
      .setSparkRunner(
        SparkRunnerInfo.getDefaultInstance)
    val controller = StreamControllerInfo.newBuilder()
      .setClsName("ksb.csle.component.pipe.controller.StreamingGenericController")
      .setStreamGenericController(
        SimpleBatchOrStreamControllerInfo.getDefaultInstance)

    val dataEngineInfo = StreamPipeEngineInfo.newBuilder()
      .setController(controller)
      .setReader(reader1)
      .setWriter(writer1)
      .setRunner(runner)
      .build

    val runType = RunType.newBuilder()
      .setId(1)
      .setPeriodic(Periodic.ONCE)
      .build()

    WorkflowInfo.newBuilder()
      .setBatch(true)
      .setMsgVersion("v0.8")
      .setKsbVersion("v1.0")
      .addRuntypes(runType)
      .addEngines(
        EngineInfo.newBuilder()
          .setId(1)
          .setPrevId(0)
          .setEngineNickName("StreamPipeEngine")
          .setStreamPipeEngine(dataEngineInfo))
      .build
  }
}
