package ksb.csle.component.operator.analysis

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types._
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.proto.BatchControlProto.BatchOperatorInfo
import ksb.csle.common.proto.BatchOperatorProto.DLTrainOperatorInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that specifies path information for DL model
 * and additional parameters to be used in the python code.
 * modelPath will be passed as '--input' parameter to python code
 * and additional parameters will be passed to key-value pairs with '--'.
 *
 * @param o Object that contains  message
 *          [[ksb.csle.common.proto.StreamOperatorProto.AggregateInfo]]
 *          AggregateInfo contains attributes as follows:
 *          - modelPath: Path for dl model.
 *                       The path should contain checkpoint and model.ckpt files (required).
 *          - attributes: key-value pairs to be passed to python code (repeated).
 *
 *  ==DLTrainOperatorInfo==
 *  {{{
 *  message DLTrainOperatorInfo {
 *   required string modelPath = 3;
 *   repeated ParamPair additionalParams = 5;
 *  }
 *  }}}
 */

class DLTrainOperator(
    o: BatchOperatorInfo) extends BaseGenericOperator[BatchOperatorInfo, Any](o){

  private[csle] val dLTrainInfo: DLTrainOperatorInfo = o.getDlTrainer

  /**
   * Dummy operator.
   *
   * @param  runner     BaseRunner to run
   * @param  df         Input dataframe
   * @return DataFrame  Output dataframe
   */
  override def operate(df: Any): Unit = ()
}

object DLTrainOperator extends Logging {
  def apply(o: BatchOperatorInfo): DLTrainOperator = new DLTrainOperator(o)
}
