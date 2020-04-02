package ksb.csle.component.controller

import org.apache.spark.sql.SparkSession

import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.proto.WorkflowProto.BatchControllerInfo

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that simply pipelines data from reader to writer
 * without any operators. This controller mostly run with batch-style engine.
 *
 * @tparam T     Data type class to be passes through data pipeline
 *               from reader to operators
 * @tparam R     Data type for the final data format
 * @param runner Runner object that is executed when engine starts
 * @param i      Reader object that reads data from data source
 * @param o      Writer object that writes pipelined data from operators
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining
 */
final class BatchDummyController[T, R](
    override val runner: BaseRunner[SparkSession, _, _],
    override val i: BaseReader[T, _, SparkSession],
    override val o: BaseWriter[R, _, SparkSession],
    override val p: BatchControllerInfo
  ) extends SparkSessionController[T, R](runner, i, o, p) {
}
