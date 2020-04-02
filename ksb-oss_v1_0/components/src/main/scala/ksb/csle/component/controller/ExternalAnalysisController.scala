package ksb.csle.component.controller

import scala.collection.JavaConversions._
import com.google.protobuf.Message

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.sql.DataFrame
import org.apache.commons.io.FilenameUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.scala.Logging

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto.BatchRunnerInfo
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.resolver.PathResolver
import ksb.csle.common.base.controller.BaseGenericController
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.{Doer, StaticDoer}
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.controller.BaseController

import ksb.csle.component.runner.analysis.TensorflowRunner
import ksb.csle.component.operator.analysis.DLTrainOperator
import ksb.csle.component.reader.FileReader
import ksb.csle.component.writer.FileWriter

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that launches external system, tensorflow.
 * This controler run with [[TensorflowRunner]] on top of batch-style engine.
 *
 * @tparam T     Data type class
 * @param runner TensorflowRunner object that calls process to launch tensorflow.
 * @param i      FileReader object that carries input file path,
 *               which will be passed as an argument, '--input' to tensorflow.
 * @param o      FileWriter object that carries output file path,
 *               which will be passed as '--output' argument to tensorflow.
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining.
 * @param ops    a list of [[DLTrainingOperators]] which carries path for model file.
 *               This path will be passed as '--model' argument to tensorflow.
 */
final class ExternalAnalysisController[T](
    val runner: TensorflowRunner[BatchRunnerInfo],
    val i: FileReader,
    val o: FileWriter,
    val p: BatchControllerInfo,
    val ops: List[DLTrainOperator]
    ) extends BaseController[T, Any] {

  val ssIn: SparkSession = runner.getSession.asInstanceOf[SparkSession]
  val ssOut: SparkSession = runner.getSession.asInstanceOf[SparkSession]
  var baseUrl:String = null
  val basePath = ops.get(0).dLTrainInfo.getModelPath

  val basePathTmp = basePath.replaceAll("FILE://", "file://")
  if (basePathTmp.contains("file://")) {
    val tmp = basePathTmp.replaceAll("file://", "")
    baseUrl = "file:///" + FilenameUtils.removeExtension(tmp)
  }else{
    baseUrl = "/" + FilenameUtils.removeExtension(basePath)
  }
  logger.info(baseUrl)
  val input_path:String = i.fileReaderInfo.getFilePath(0).replace("////", "///")
  val model:String = PathResolver.getPathByModelManager(baseUrl, true)
  val outputPath:String = o.fileWriterInfo.getFilePath(0).replace("////", "///")
  val output_Path:String = PathResolver.getPathByModelManager(outputPath, true)
  logger.info(s"--input: $input_path")
  logger.info(s"--modelPath: $basePath")
  logger.info(s"--model: $model")
  logger.info(s"--output: $output_Path")
  val params: String = Seq(
      "--input", input_path,
      "--output", output_Path,
      "--model", model,
      ProtoUtils.msgToParamString(ops.get(0).dLTrainInfo)).mkString(" ")

  runner.init(params)

  /**
   * Progress job by calling tensorflow python application.
   */
  override def progress: Any = runner.run("dataset")

  override def stop: Unit = ()
}
