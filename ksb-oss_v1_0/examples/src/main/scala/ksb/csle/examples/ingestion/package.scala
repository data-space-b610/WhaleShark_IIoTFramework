package ksb.csle.examples

package object ingestion {
  def batchDummyControllerClassName =
    "ksb.csle.component.controller.BatchDummyController"

  def streamToStreamControllerClassName =
    "ksb.csle.component.controller.SparkSessionOrStreamController"

//  def streamControllerClassName =
//    "ksb.csle.component.controller.DefaultStreamController"

  def runnerClassName =
    "ksb.csle.component.runner.SimpleSparkRunner"

  def fileReaderClassName =
    "ksb.csle.component.reader.FileReader"

  def httpServerReaderClassName =
    "ksb.csle.component.reader.HttpServerReader"

  def kafkaReaderClassName =
    "ksb.csle.component.reader.KafkaReader"

  def stdoutWriterClassName =
    "ksb.csle.component.writer.StdoutWriter"

  def mongodbWriterClassName =
    "ksb.csle.component.writer.MongodbWriter"

  def phoenixWriterClassName =
    "ksb.csle.component.writer.PhoenixWriter"

  def tableWriterClassName =
    "ksb.csle.component.writer.TableWriter"

  def kafkaWriterClassName =
    "ksb.csle.component.writer.KafkaWriter"

  def onem2mHttpServerReaderClassName =
    "ksb.csle.component.reader.Onem2mHttpReader"
}
