package ksb.csle.component.controller

import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.base.controller.BaseOnDemandServingController
import ksb.csle.common.base.operator.BaseGenericMutantOperator
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that simply pipelines DataFrame from one operator to another.
 * This controller runs with TensorflowServingRunner
 * on top of on-demand serving engine.
 *
 * @tparam T Data type.
 * @tparam R Result type.
 *
 * @param runner Runner for tensorflow model serving.
 *
 * @param p      Object that contains message
 *               [[ksb.csle.common.proto.WorkflowProto.SimpleOnDemandControllerInfo]]
 *               SimpleOnDemandControllerInfo contains no attribute.
* @param ops    a series of [[BaseGenericMutantOperator]] objects that processes data
 *               one by one. Operators for tensorflow model serving.
 */
final class TensorflowServingController[T, R](
    override val runner: BaseRunner[_, _, _],
    override val p: OnDemandControllerInfo,
    override val ops: List[BaseGenericMutantOperator[_, R, R]]
    ) extends BaseOnDemandServingController[
    T, OnDemandControllerInfo, R](runner, p, ops) {

  /**
   * Start tensorflow model serving.
   *
   * @return BaseResult Result data.
   */
  override def serve(): BaseResult = {
    runner.init(ops.head)
    runner.run()
    DefaultResult("s","p","o").asInstanceOf[BaseResult]
  }

  /**
   * Stop tensorflow model serving.
   *
   * @return Any Result data.
   */
  override def stop: Any = {
    super.stop()
  }
}
