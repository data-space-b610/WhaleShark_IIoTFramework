package ksb.csle.component.controller

import java.lang.reflect.ParameterizedType

import scala.collection.JavaConversions._

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord

import ksb.csle.common.proto.WorkflowProto.{BatchControllerInfo, StreamControllerInfo}
import ksb.csle.common.utils._
import ksb.csle.common.base.{Doer, StaticDoer}
import ksb.csle.common.base.controller.BaseGenericController
import ksb.csle.common.base.operator.BaseGenericOperator
import ksb.csle.common.base.reader.BaseReader
import ksb.csle.common.base.writer.BaseWriter
import ksb.csle.common.base.runner.BaseRunner
import ksb.csle.common.base.result.BaseResult
import ksb.csle.common.base.controller.BaseController
import ksb.csle.common.base.result.DefaultResult

/**
 * :: ApplicationDeveloperApi ::
 *
 * Controller that simply pipelines DataFrame from reader to writer
 * via a series of operators.
 * This controller can be run both on streaming engine and batch-style engine,
 * and work as batch and streaming processing controller.
 *
 * @tparam T     Data type class to be passes through data pipeline
 *               from reader to operators
 * @tparam R     Data type for the final data format
 * @tparam P     Message type class
 * @tparam SI    Session type class for reader object
 * @tparam SO    Session type class for writer object
 * @param runner Runner object that runs spark session
 * @param i      Reader object that reads data from data source
 * @param o      Writer object that writes pipelined data from operators
 * @param p      Message object that has one of messages object containing
 *               attributes related to controlling process pipelining
 * @param ops    a list of [[BaseGenericOperator]] objects that processes data
 *               one by one.
 */
final class SparkSessionOrStreamController[T, R, P, SI, SO](
    override val runner: BaseRunner[SI, _, _],
    override val i: BaseReader[T, _, SI],
    override val o: BaseWriter[R, _, SO],
    override val p: P,
    override val ops: List[BaseGenericOperator[ _, R]]
    ) extends BaseGenericController[T, P, R, SI, SO](runner, i, o, p, ops) {

  private val info = p match {
    case x: StreamControllerInfo => x.getSparkSessionOrStreamController
    case x: BatchControllerInfo => x.getSparkSessionOrStreamController
    case _ =>
      throw new ClassCastException(s"Unsupported controller parameter error.")
  }

  private val ssIn: SparkSession = runner.getSession.asInstanceOf[SparkSession]
  private val ssOut: SparkSession = runner.getSession.asInstanceOf[SparkSession]
  logger.info("operationPeriod : " + info.getOperationPeriod)
  private val sscIn: StreamingContext = SparkUtils.getStreamingContext(ssIn,ssIn.sparkContext.master,info.getOperationPeriod)
  private val sscOut: StreamingContext = SparkUtils.getStreamingContext(ssOut,ssOut.sparkContext.master,info.getOperationPeriod)

  /**
   * Pipelines data from reader to writer via a series of operators.
   */
  override def process(): BaseResult = {
    execute(i, o)
    // FIXME: Seperate sparksession controller and sparkstreaming controller.
    if (SparkUtils.hasOutputOperations(sscIn)) {
      SparkUtils.startStreaming(sscIn)
      logger.info("SparkStreamingContext started")
      SparkUtils.awaitStreamingTermination(sscIn)
    }
    DefaultResult("s", "p", "o").asInstanceOf[BaseResult]
  }

  private def execute(reader: BaseReader[T, _, _], writer: BaseWriter[R, _, _]) {
    val superClass = reader.getClass.getGenericSuperclass
    val sessionType = superClass.asInstanceOf[ParameterizedType]
      .getActualTypeArguments()(2)

    val input = sessionType.getTypeName match {
      case "org.apache.spark.sql.SparkSession" =>
        reader.asInstanceOf[BaseReader[T, _, SparkSession]].read(ssIn)
      case "org.apache.spark.streaming.StreamingContext" =>
        reader.asInstanceOf[BaseReader[T, _, StreamingContext]].read(sscIn)
      case _ =>
        throw new RuntimeException("Not supported session type.")
    }

    input match {
      case df: Dataset[_] =>
        writer.write(pipeOperators(runner, ops, df.asInstanceOf[R]))
      case dstream: InputDStream[_] =>
        dstream.foreachRDD { rdd =>
          if (!rdd.isEmpty()) {
            val jsonRDD = rdd.map { e =>
              e match {
                case json: String => json
                case record: ConsumerRecord[_, _] => record.value().toString()
                case _ => e.toString()
              }
            }
            val df = ssIn.sqlContext.read.json(jsonRDD)
            writer.write(pipeOperators(runner, ops, df.asInstanceOf[R]))
          }
        }
    }
  }

  override def stop: Any = {
    sscIn.getState() match {
      case StreamingContextState.ACTIVE =>
        try {
          sscIn.stop()
        } catch {
          case e: Exception =>
            logger.warn(s"SparkStreamingContext stop error: $e")
        }
      case _ =>
        // do nothing, streaming is not started.
    }

    super.stop()
  }
}
