package ksb.csle.component.runner

import org.apache.spark.sql.SparkSession
import com.google.protobuf.Message

import ksb.csle.common.proto.RunnerProto.{BatchRunnerInfo, StreamRunnerInfo}
import ksb.csle.common.proto.RunnerProto.SparkRunnerInfo
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.utils.SparkUtils

/**
 * :: ApplicationDeveloperApi ::
 *
 * Runner that gets session which derived from Spark context created by YARN.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.RunnerProto.SparkRunnerInfo]]
 *          [[SparkRunnerInfo]] containing attributes are as follows:
 *          - sparkArgs: Spark argument message to be passed to spark (optional)
 *                       Spark arguement message contains belowed parameters.
 *          - inJson: method parameter passing to spark (deprecated)
 *                    As of KSB v0.8.1, this paramter does not need to set any more.
 *
 * ==SparkRunnerInfo==
 * {{{
 * message SparkRunnerInfo {
 *   optional SparkArgs sparkArgs = 1;
 *   optional bool inJson = 2 [default = false];
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

class SimpleSparkRunner[P](
    o: P) extends BaseRunner[SparkSession, P, Any](o) {

  protected val p: SparkRunnerInfo = o match {
    case x: BatchRunnerInfo => x.getSparkRunner
    case x: StreamRunnerInfo => x.getSparkRunner
    case _  =>
      throw new IllegalArgumentException("not supported argument type.")
  }

  protected val spark: SparkSession = p match {
    case x: SparkRunnerInfo =>
      SparkUtils.getSparkSession(x.getSparkArgs.getAppName, x.getSparkArgs.getMaster)
    case _  =>
      throw new IllegalArgumentException("not supported argument type.")
  }

  /**
   * @returns SparkSession
   */
  override def getSession: SparkSession = {
    import spark.implicits._
    spark
  }

  override def init(a: Any): Unit = ()

  override def run(params: Any): Unit = ()

  override def stop: Unit = spark.stop()
}
