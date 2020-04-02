package ksb.csle.component.runner

import scala.sys.process._

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import com.google.protobuf.Message

import ksb.csle.common.utils.config.ConfigUtils
import ksb.csle.common.utils.ProtoUtils
import ksb.csle.common.base.runner.BasePredictiveAnalysisRunner
import ksb.csle.common.proto.RunnerProto.BatchRunnerInfo
import ksb.csle.common.proto.AutoSparkMlProto.AutoSparkMLInfo
import ksb.csle.common.proto.RunnerProto.PySparkMLRunnerInfo
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Runner that gets session which derived from Spark context created by YARN.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.RunnerProto.SparkRunnerInfo]]
 *          PySparkRunnerInfo containing attributes are as follows:
 *          - pyEntryPath: python file path for spark driver code. (required)
 *                     Default path value ("/pyML/autosparkml/bridge/call_trainer.py")
 *                     is highly recommended.
 *          - sparkArgs: Spark argument message to be passed to spark (optional)
 *                       Spark arguement message contains belowed parameters.
 *          - inJson: method parameter passing to spark (deprecated)
 *                    As of KSB v0.8.1, this paramter does not need to set any more.
 *
 * ==SparkRunnerInfo==
 * {{{
 * message SparkRunnerInfo {
 *  required string pyEntryPath = 1 [default = "/pyML/autosparkml/bridge/call_trainer.py"];
 *  optional SparkArgs sparkArgs = 1;
 *  optional bool inJson = 2 [default = false];
 * }
 * }}}
 *          - appName: Spark application name (Deprecated)
 *          - master:  Master URL passed to Spark (Deprecated)
 *                     This can be in one of formats in following reference link:
 *          - deployMode: cluster or client (not used)
 *          For more detailed information, please refer to following site:
 *          [[https://spark.apache.org/docs/latest/submitting-applications.html]]
 *
 * ==SparkArgsInfo==
 * {{{
 * message SparkArgs {
 *   optional string appName = 1;
 *   optional string master =  2 [default = "local[*]"];
 *   optional string deployMode = 3;
 *   optional string mainClass = 4;
 *   optional string conf = 5;
 *   optional string executorCores = 7;
 *   optional string memory = 8;
 *   optional string driverMemory = 9;
 *   optional string executerMemory = 10;
 *   optional string numExecutors = 11;
 *   optional string totalCores = 12;
 *   optional string extraArgs = 13;
 *   repeated string confOptions = 14;
 *   optional string jars = 15;
 *   optional string pyFiles = 16;
 *   optional string mainJar = 17;
 *   optional string sparkVersion = 18 [default = "2.1.1"];
 * }
 * }}}
 */
@Deprecated
final class PySparkRunner[P](
    o: BatchRunnerInfo) extends BasePredictiveAnalysisRunner[Any, BatchRunnerInfo, Any](o) {

  @transient private val p: PySparkMLRunnerInfo = o match {
    case x: BatchRunnerInfo => x.getPySparkMLRunner
    case _  =>
      throw new IllegalArgumentException("not supported argument type.")
  }

  private var command: Seq[String] = PySparkRunner.createCommandLine(p.getPyEntryPath)
  private var params: String = null

  override def getSession: Any = ()

  override def init(_params: Any): Any = {
//    command = PySparkRunner.createCommandLine(_params)

    params = _params match {
      case x: AutoSparkMLInfo =>
          if (p.getInJson)
            " --proto \"" + ProtoUtils.msgToJson(
                x.asInstanceOf[Message]).replace(" ", ""
                    ).replace("\"", "\\\"") + "\""
          else
            PySparkRunner.createStringParams(x)
      case x: String => x
      case _  =>
        throw new IllegalArgumentException("not supported argument type.")
    }
    logger.info("PySpark parameters initialized !")
  }

  override def trainOrPredict(df: Any): Any = {
    val process = Process(
         (command :+ params).mkString(" ")).lineStream.foreach(logger.info(_))
    logger.info("PySpark runner Ok!")
    DefaultResult("s", "p", "o")
  }

  override def stop: Unit = ()
}

@Deprecated
object PySparkRunner {

  // TODO: Move to ksb.csle.common.utils.
  private def appendToEnv(key: String, value: String) = scala.util.Properties.envOrNone(key) match {
    case Some(v) if v.nonEmpty => s"$v${System getProperty "path.separator"}$value"
    case _ => value
  }

  private def createStringParams(p: Any) = {
    val params = Seq(
//        "--input", p.getDataInfo.getInputDataPath,
//        "--method", p.getMethod.name(),
//        "--crossvalidator", p.getCrossValidator.getMethod.name(),
//        "--model", p.getDataInfo.getOutputModelPath()
        ).mkString(" ")
    params
  }

  private[component] def createCommandLine(pyEntryPath: String): Seq[String] = {
    val configurator = ConfigUtils.getConfig()
    val pythonPath: String = configurator.envOrElseConfig("csle.user.home")+configurator.envOrElseConfig("servers.pyspark.python_path")
    val codePath = configurator.envOrElseConfig("csle.user.home")+pyEntryPath
    List(pythonPath, codePath )
  }
}
