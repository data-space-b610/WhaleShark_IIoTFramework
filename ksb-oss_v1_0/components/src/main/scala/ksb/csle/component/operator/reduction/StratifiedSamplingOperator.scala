package ksb.csle.component.operator.reduction

import scala.collection.JavaConversions.asScalaBuffer

import com.google.protobuf.Message
import org.apache.spark.sql.{Row, SparkSession, DataFrame, SQLContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD

import ksb.csle.common.base.operator.BaseDataOperator
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.proto.StreamOperatorProto._
import ksb.csle.common.proto.StreamOperatorProto.SampleStratifiedInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.component.ingestion.util._
import ksb.csle.component.exception._
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that performs 'forward selection' which is the deterministic greedy
 * feature selection algorithm. It selects the most relevant columns.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.SampleStratifiedInfo]]
 *          SampleStratifiedInfo contains attributes as follows:
 *          - selectedColumnId: Column ids to be applied by stratified-sampling
 *                             (required)
 *          - fractions: Map of specific keys to sampling rates (required)
 *          - withReplacement: Parameter to specify whether to sample with or
 *                             without replacement. (required)
 *          - seed: Local random seed(optional)
 *
 *  ==SampleStratifiedInfo==
 *  {{{
 *  message SampleStratifiedInfo {
 *  required int32 selectedColumnId = 3;
 *  repeated FractionFieldEntry fractions = 4;
 *  required bool withReplacement = 5 [default = false];
 *  optional int64 seed = 6;
 *  }
 *  }}}
 */
class StratifiedSamplingOperator(
    o: StreamOperatorInfo
    ) extends BaseDataOperator[StreamOperatorInfo, DataFrame](o) {

  val p: ksb.csle.common.proto.StreamOperatorProto.SampleStratifiedInfo =
    o.getSampleStratified

  /**
   * validates sampleStratified info and dataframe schema info.
   */
  @throws(classOf[KsbException])
  def validate(df: DataFrame): DataFrame = {
    try {
      if (p.getFractionsList.size <= 1)
        throw new DataException(
            s"Too small Number Of Fractions Data : $p.getFractionsList.size")
      if (p.getSelectedColumnId < 0 || p.getSelectedColumnId >= df.schema.size)
        throw new DataException(
            s"The Selected Column dose not exist : $p.getSelectedColumnId")
    } catch {
      case e: DataException => throw e
      case e: Exception => throw new DataException(s"validate Error : ", e)
    }
    df
  }

  /**
   * Run sampleStratified operation using following params.
   */
  @throws(classOf[KsbException])
  private def sampleStratified(df: DataFrame): DataFrame = {
    logger.info(s"OpId ${o.getId} : SampleStratified")
    val withReplacement: Boolean = p.getWithReplacement
    val selectedColumnId: Int = p.getSelectedColumnId
    val seed: Long = p.getSeed
    val fractions: Map[Any , Double] = p.getFractionsList.map(
        x => x.getKey -> x.getValue).toMap
    var result = validate(df)
    import df.sqlContext.implicits._
    var sample: RDD[Row] = null
    try {
      if(seed == 0) {
        sample = result.rdd.keyBy(
            x=>x(selectedColumnId)).sampleByKey(false, fractions).values
      }
      else {
        sample = result.rdd.keyBy(
            x=>x(selectedColumnId)).sampleByKey(false, fractions, seed).values
      }
      result = df.sparkSession.createDataFrame(sample, df.schema)
    } catch {
      case e: Exception => throw new ProcessException(
          s"SampleStratified Process Error : ", e)
    }
    logger.info(result.show.toString)
    result
  }

  /**
   * Operates sampleStratified.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  def operate(df: DataFrame): DataFrame = sampleStratified(df)
}

object StratifiedSamplingOperator {
  def apply(o: StreamOperatorInfo): StratifiedSamplingOperator =
    new StratifiedSamplingOperator(o)
}