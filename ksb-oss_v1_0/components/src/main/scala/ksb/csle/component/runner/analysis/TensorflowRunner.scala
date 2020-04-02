package ksb.csle.component.runner.analysis

import java.util.Date
import java.text.SimpleDateFormat
import java.net.URL

import com.google.protobuf.Message

import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler._
import org.apache.commons.io.FilenameUtils

import ksb.csle.common.proto.RunnerProto.BatchRunnerInfo
import ksb.csle.common.proto.RunnerProto.TensorflowRunnerInfo
import ksb.csle.common.proto.BatchOperatorProto.DLTrainOperatorInfo
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.utils.SparkUtils
import ksb.csle.common.utils.NativeProcessUtil
import ksb.csle.common.utils.resolver.PathResolver
import ksb.csle.common.utils.HdfsUtils

import TensorflowRunner._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Runner that creates command-line and calls tensorflow with python code
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.RunnerProto.TensorflowRunnerInfo]]
 *          TensorflowRunnerInfo containing attributes are as follows:
 *          - pyEntryPath: Python file path for python code that will be called
 *                     by process (required)
 *          - cluster: Mode flag that indicates tensorflow cluster.
 *                     Default value is false.
 *                     If it is true, distributed tensorflow model will be used.
 *                     If you set CODDL-enable tensorflow cluster, you can set as true,
 *                     but it's not, you should set it false.
 *          - host:   A series of hosts consisting of Tensorflow cluster
 *                    (optional/repeated).
 *          - inJson: Flag that indicates the way to pass parameter to tensorflow.
 *                    If it is true, json-style parameters to be passed to tensorflow,
 *                    and it is false, command-line parameter key-value pair will be passed.
 *          - tfVersion: Tensorflow version (optional)
 *
 * ==TensorflowRunnerInfo==
 * {{{
 * message TensorflowRunnerInfo {
 *  required string pyEntryPath = 1;
 *  optional bool cluster = 2 [default = false];
 *  repeated string host = 3;
 *  optional bool inJson = 4 [default = false];
 *  optional string tfVersion = 5 [default = "r1.3"];
 * }
 * }}}
 */
final class TensorflowRunner[P](
    o: P) extends BaseRunner[SparkSession, P, Any](o) {

  @transient private val p: TensorflowRunnerInfo = o match {
    case x: BatchRunnerInfo => x.getTensorflowRunner
    case _ =>
      throw new IllegalArgumentException("not supported argument type.")
  }

  private val appName = createAppName(p.getPyEntryPath)
  private val command: Seq[String] = createCommandLine(p.getPyEntryPath)
  private var params: Seq[String] = null
  private val spark = SparkUtils.getSparkSession(appName)

  override def getSession: SparkSession = spark

  override def init(_params: Any): Any = {
    params = _params match {
      case x: DLTrainOperatorInfo =>
        if (p.getInJson) {
          Seq(ProtoUtils.msgToJson(x.asInstanceOf[Message]))
        } else {
          ProtoUtils.msgToParamString(x.asInstanceOf[Message]).split(" ")
        }
      case s: String =>
        s.split(" ")
      case _ =>
        throw new IllegalArgumentException("not supported argument type.")
    }
    logger.debug(s"TensorflowRunner init params: $params")
  }

  /**
   * Executes process call by command-line when controller calls this.
   * @param data Data
   * @return Any value
   */
  override def run(data: Any): Any = {
    logger.info(s"TensorflowRunner is starting: $appName, $command, $params")

    // kill a tensorflow application when spark application is terminated.
    spark.sparkContext.addSparkListener(
      new SparkListener() {
        override def onApplicationEnd(
          applicationEnd: SparkListenerApplicationEnd): Unit = {
          NativeProcessUtil.kill(appName)
          logger.info(s"TensorflowRunner is stopped: $appName")
        }
      })

    val appCommand =
      if (params == null) command
      else command ++: params
    val workingDir = sys.env("KSB_HOME")
    logger.info(s"submit a tensorflow application: $appName, $appCommand")
    NativeProcessUtil.submit(appName, appCommand, workingDir, logger)

    DefaultResult("s", "p", "o")
  }

  override def stop: Unit = ()
}

object TensorflowRunner {
  def createAppName(pythonCodePath: String): String = {
    val time = new SimpleDateFormat("yyyyMMdd-hhmmss").format(new Date())
    val index = pythonCodePath.lastIndexOf('/')
    if (index < 0) {
      pythonCodePath + "_" + time
    } else {
      pythonCodePath.substring(index + 1) + "_" + time
    }
  }

  def createCommandLine(pythonCodePath: String): Seq[String] = {
    var tmpPath: String = null
    var hdfsSrcPath: String = null
    var localDstPath: String = null
    val configurator = ConfigUtils.getConfig()
    val codePath =
      if (pythonCodePath.startsWith("file://"))
        pythonCodePath.replace("////", "///").replace("file://", "")
      else if (pythonCodePath.startsWith("hdfs://")) {
        val hdfsAddress = pythonCodePath.replaceAll("hdfs://", "").split("/")
        val hdfsHost = hdfsAddress(0).split(":")
        val hdfsHostName = hdfsHost(0)
        val hdfsHostPort = hdfsHost(1)
        val hdfsbasePath = "/" + hdfsAddress.drop(1).mkString("/")
        val hdfsIO = new HdfsUtils(hdfsHostName, hdfsHostPort.toInt)
        hdfsSrcPath = FilenameUtils.getFullPath(hdfsbasePath)
        localDstPath = "/home/csle/ksb-csle/components/src/main/python"
        val pyCodeName = hdfsbasePath.split("/").last
        val localPyPath = localDstPath + "/" + hdfsSrcPath.split("/").last + "/" + pyCodeName
        println(s"hdfsHostName: ${hdfsHostName}")
        println(s"hdfsHostPort: ${hdfsHostPort}")
        println(s"hdfsbasePath: ${hdfsbasePath}")
        println(s"hdfsSrcPath: ${hdfsSrcPath}")
        println(s"localDstPath: ${localDstPath}")
        println(s"localPyPath: ${localPyPath}")
        println(s"pyCodeName: ${pyCodeName}")
        hdfsIO.copyHDFStoLocal(hdfsSrcPath, localDstPath, pyCodeName, false)
        localPyPath
      } else
        configurator.envOrElseConfig("csle.home") + pythonCodePath

    Seq(configurator.envOrElseConfig("csle.user.home")
      + configurator.envOrElseConfig("servers.tensorflow.python_path"),
      codePath)
  }

}
