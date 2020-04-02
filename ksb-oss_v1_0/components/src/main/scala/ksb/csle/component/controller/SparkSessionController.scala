package ksb.csle.component.controller

import scala.collection.JavaConversions._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

import ksb.csle.common.proto.WorkflowProto.BatchControllerInfo
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.{Doer, StaticDoer}
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.controller.BaseGenericController
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that simply pipelines DataFrame from reader to writer
 * via a series of operators. This controller mostly run with batch-style engine.
 *
 * @tparam T     Data type class to be passes through data pipeline
 *               from reader to operators
 * @tparam R     Data type for the final data format
 * @param runner Runner object that runs spark session
 * @param i      Reader object that reads data from data source
 * @param o      Writer object that writes pipelined data from operators
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining
 * @param ops    a list of [[BaseGenericOperator]] objects that processes data
 *               one by one.
 */
class SparkSessionController[T, R](
    override val runner: BaseRunner[SparkSession,_,_],
    override val i: BaseReader[T,_,SparkSession],
    override val o: BaseWriter[R,_,SparkSession],
    override val p: BatchControllerInfo,
    override val ops : List[BaseGenericOperator[ _, R]]
    ) extends BaseGenericController[T, BatchControllerInfo, R, SparkSession, SparkSession](runner, i, o, p, ops) {

  def this(runner: BaseRunner[SparkSession, _, _],
    i: BaseReader[T, _, SparkSession],
    o: BaseWriter[R, _, SparkSession],
    p: BatchControllerInfo,
    op: BaseGenericOperator[_, R]) = this(runner, i, o, p, List(op))

  def this(runner: BaseRunner[SparkSession, _, _],
    i: BaseReader[T, _, SparkSession],
    o: BaseWriter[R, _, SparkSession],
    p: BatchControllerInfo) = this(runner, i, o, p, Nil)

  runner.init(_)
  val sessionIn: SparkSession= runner.getSession
  val sessionOut: SparkSession = runner.getSession

  /**
   * Pipelines data from reader to writer via a series of operators.
   */
  override def process(): BaseResult = {
    // TODO: merge if more than one files exist.
    val input = i.read(sessionIn)
    val output = pipeOperators(runner, ops, input.asInstanceOf[R])
    o.write(sessionOut, output)

    DefaultResult("s","p","o").asInstanceOf[BaseResult]
  }

  override def stop: Any = {
    super.stop()
  }
}
