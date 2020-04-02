package ksb.csle.component.operator.reduction

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.SampleSimpleRandomInfo
import ksb.csle.component.ingestion.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that creates simple random samples from a selected column.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SampleSimpleRandomInfo]]
 *          SampleSimpleRandomInfo contains attributes as follows:
 *          - fractions: map of specific the sampling rate (required)
 *                       If the fraction value is 1, the result dataframe is
 *                       same as the original dataframe.
 *          - withReplacement: Parameter to specify whether to sample with or
 *                       without replacement (required)
 *                       If withReplacement is false, the result dataframe does
 *                       not have the same rows.
 *          - seed: Local random seed (optional)
 *
 *  ==SampleSimpleRandomInfo==
 *  {{{
 *  message SampleSimpleRandomInfo {
 *  required double fraction = 3;
 *  required bool withReplacement = 4 [default = false];
 *  optional int64 seed = 5;
 *  }
 *  }}}
 */
class RandomSamplingOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.SampleSimpleRandomInfo =
    o.getSampleSimpleRandom

  /**
   * Run SampleSimpleRandom operation using following params.
   */
  @throws(classOf[KsbException])
  private def sampleSimpleRandom(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : SampleSimpleRandom")
    val fraction: Double  = p.getFraction
    val withReplacement: Boolean = p.getWithReplacement
    val seed: Long = p.getSeed
    var result = df
    try {
      if(seed == 0) {
        result = result.sample(withReplacement, fraction)
      }
      else {
        result = result.sample(withReplacement, fraction, seed)
      }
    } catch {
      case e: Exception => throw new ProcessException(
          s"SampleSimpleRandom Process Error : ", e)
    }
    logger.info(result.show.toString)
    result
  }

  /**
   * Operates sampleSimpleRandom.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: DataFrame): DataFrame = sampleSimpleRandom(df)
}

object RandomSamplingOperator {
  def apply(o: StreamOperatorInfo): RandomSamplingOperator =
    new RandomSamplingOperator(o)
}