package ksb.csle.component.pipe.controller

import com.google.protobuf.Message
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.WorkflowProto.StreamControllerInfo
import ksb.csle.common.proto.RunnerProto.StreamRunnerInfo
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo
import ksb.csle.common.base.pipe.reader.BasePipeReader
import ksb.csle.common.base.pipe.writer.BasePipeWriter
import ksb.csle.common.base.pipe.operator.BaseGenericPipeOperator
import ksb.csle.common.base.pipe.controller
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.pipe.controller.BasGenericePipeController
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.result.DefaultResult
import ksb.csle.component.pipe.controller.vo._

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that manipulate reader, operator, and writer handles
 * when engine starts.
 *
 * @param runner Runner object that gets spark session
 * @param ins    A series of reader objects that read data from data source
 * @param o      A series of Writer objects that write pipelined data
 *               from operators to writers
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining
 * @param ops    a series of [[BaseGenericPipeOperator]] objects
 *               that returns pipeline handle to process.
 */
class StreamingGenericController(
  override val runner: BaseRunner[SparkSession, StreamRunnerInfo, Any],
  override val ins: Seq[BasePipeReader[DataFrame, StreamReaderInfo, SparkSession]],
  override val outs: Seq[BasePipeWriter[DataFrame, StreamingQuery, StreamWriterInfo, SparkSession]],
  override val p: StreamControllerInfo,
  override val ops: Seq[BaseGenericPipeOperator[_, _, DataFrame, StreamOperatorInfo, SparkSession]]
  ) extends BasGenericePipeController[DataFrame, DataFrame, StreamingQuery, Message, SparkSession](
      runner, ins, outs, p, ops) {

  override def init = {
    super.init()
    runner.getSession.sparkContext.setLogLevel("ERROR")
  }

  /**
   * Pipelines data from readers to writers via operators.
   * @return result
   */
  override def progress: BaseResult = {
    super.progress()
    runner.getSession.streams.awaitAnyTermination()
    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  override def stop: Any = super.stop
}
